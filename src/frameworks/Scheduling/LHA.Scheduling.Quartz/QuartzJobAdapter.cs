using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Quartz;

namespace LHA.Scheduling.Quartz;

/// <summary>
/// Bridges the LHA <see cref="IScheduledJob"/> abstraction to Quartz.NET's <see cref="IJob"/> interface.
/// Quartz triggers this adapter, which resolves the actual <see cref="IScheduledJob"/> from DI.
/// </summary>
/// <remarks>
/// This is a generic adapter — one instance per job type. Quartz invokes it via
/// <see cref="IJobDetail.JobType"/> which will be <c>QuartzJobAdapter&lt;TJob&gt;</c>.
/// The adapter creates a DI scope, resolves TJob, builds a <see cref="JobContext"/>,
/// and delegates execution.
/// </remarks>
[DisallowConcurrentExecution]
[PersistJobDataAfterExecution]
public sealed class QuartzJobAdapter<TJob> : IJob where TJob : IScheduledJob
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<QuartzJobAdapter<TJob>> _logger;

    public QuartzJobAdapter(
        IServiceScopeFactory scopeFactory,
        ILogger<QuartzJobAdapter<TJob>> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    public async ValueTask Execute(
        IJobExecutionContext quartzContext,
        CancellationToken cancellationToken = default)
    {
        var jobDetail = quartzContext.JobDetail;
        var dataMap = quartzContext.MergedJobDataMap;

        var serializedParams = dataMap.GetString(QuartzDataMapKeys.SerializedParameters);
        var tenantId = dataMap.GetString(QuartzDataMapKeys.TenantId);
        var correlationId = dataMap.GetString(QuartzDataMapKeys.CorrelationId);
        var userId = dataMap.GetString(QuartzDataMapKeys.UserId);
        var retryAttempt = dataMap.ContainsKey(QuartzDataMapKeys.RetryAttempt)
            ? dataMap.GetInt(QuartzDataMapKeys.RetryAttempt)
            : 0;
        var maxRetries = dataMap.ContainsKey(QuartzDataMapKeys.MaxRetries)
            ? dataMap.GetInt(QuartzDataMapKeys.MaxRetries)
            : 3;

        _logger.LogInformation(
            "Executing job [{JobType}] FireInstanceId [{FireId}] TenantId [{TenantId}]",
            typeof(TJob).Name, quartzContext.FireInstanceId, tenantId);

        using var scope = _scopeFactory.CreateScope();
        var job = scope.ServiceProvider.GetRequiredService<TJob>();

        // Extract metadata from data map
        var metadata = new Dictionary<string, string>();
        foreach (var key in dataMap.Keys)
        {
            if (key.StartsWith("meta.", StringComparison.OrdinalIgnoreCase))
            {
                metadata[key[5..]] = dataMap.GetString(key) ?? string.Empty;
            }
        }

        var context = new JobContext
        {
            ExecutionId = quartzContext.FireInstanceId,
            JobId = jobDetail.Key.ToString(),
            JobType = typeof(TJob).AssemblyQualifiedName!,
            TenantId = tenantId,
            CorrelationId = correlationId,
            UserId = userId,
            RetryAttempt = retryAttempt,
            SerializedParameters = serializedParams,
            CancellationToken = cancellationToken,
            Metadata = metadata,
            SchedulerMetadata = new Dictionary<string, object>
            {
                [QuartzMetadataKeys.FireInstanceId] = quartzContext.FireInstanceId,
                [QuartzMetadataKeys.JobKey] = jobDetail.Key.ToString(),
                [QuartzMetadataKeys.TriggerKey] = quartzContext.Trigger.Key.ToString(),
                [QuartzMetadataKeys.ScheduledFireTimeUtc] = quartzContext.ScheduledFireTimeUtc?.ToString("O") ?? string.Empty
            }
        };

        var result = await job.ExecuteAsync(context);

        if (result.Success)
        {
            _logger.LogInformation(
                "Job [{JobType}] completed successfully: {Message}",
                typeof(TJob).Name, result.Message);
            // Reset retry counter on success
            quartzContext.JobDetail.JobDataMap[QuartzDataMapKeys.RetryAttempt] = 0;
        }
        else if (result.ShouldRetry && retryAttempt < maxRetries)
        {
            _logger.LogWarning(result.Exception,
                "Job [{JobType}] failed (attempt {Attempt}/{MaxRetries}), scheduling retry: {Message}",
                typeof(TJob).Name, retryAttempt + 1, maxRetries, result.Message);

            // Increment retry counter
            quartzContext.JobDetail.JobDataMap[QuartzDataMapKeys.RetryAttempt] = retryAttempt + 1;

            // Schedule a retry with exponential backoff
            var backoff = TimeSpan.FromSeconds(Math.Pow(2, retryAttempt) * 5);
            throw new JobExecutionException(
                result.Message ?? "Job failed",
                result.Exception ?? new InvalidOperationException(result.Message ?? "Job failed"));
        }
        else
        {
            _logger.LogError(result.Exception,
                "Job [{JobType}] failed permanently after {Attempts} attempt(s): {Message}",
                typeof(TJob).Name, retryAttempt + 1, result.Message);
        }
    }
}
