using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Quartz;

namespace LHA.Scheduling.Quartz;

/// <summary>
/// Quartz.NET implementation of <see cref="IJobScheduler"/>.
/// Maps the scheduler-agnostic API to Quartz trigger/job abstractions.
/// </summary>
public sealed class QuartzJobScheduler : IJobScheduler
{
    private readonly ISchedulerFactory _schedulerFactory;
    private readonly QuartzSchedulingOptions _options;
    private readonly ILogger<QuartzJobScheduler> _logger;

    private const string DefaultGroup = "lha-jobs";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        WriteIndented = false,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };

    public QuartzJobScheduler(
        ISchedulerFactory schedulerFactory,
        IOptions<QuartzSchedulingOptions> options,
        ILogger<QuartzJobScheduler> logger)
    {
        _schedulerFactory = schedulerFactory;
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<string> EnqueueAsync<TJob>(
        object? parameters = null,
        JobOptions? options = null,
        CancellationToken cancellationToken = default) where TJob : IScheduledJob
    {
        var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
        var jobId = GenerateJobId();
        var (jobDetail, trigger) = BuildOneShot<TJob>(jobId, parameters, options,
            b => b.StartNow());

        await scheduler.ScheduleJob(jobDetail, trigger, default, cancellationToken);

        _logger.LogInformation(
            "Enqueued job [{JobType}] → Quartz key [{JobId}]",
            typeof(TJob).Name, jobId);

        return jobId;
    }

    /// <inheritdoc />
    public async Task<string> ScheduleAsync<TJob>(
        TimeSpan delay,
        object? parameters = null,
        JobOptions? options = null,
        CancellationToken cancellationToken = default) where TJob : IScheduledJob
    {
        var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
        var jobId = GenerateJobId();
        var startAt = DateTimeOffset.UtcNow.Add(delay);
        var (jobDetail, trigger) = BuildOneShot<TJob>(jobId, parameters, options,
            b => b.StartAt(startAt));

        await scheduler.ScheduleJob(jobDetail, trigger, default, cancellationToken);

        _logger.LogInformation(
            "Scheduled job [{JobType}] → Quartz key [{JobId}] delay [{Delay}]",
            typeof(TJob).Name, jobId, delay);

        return jobId;
    }

    /// <inheritdoc />
    public async Task<string> ScheduleAsync<TJob>(
        DateTimeOffset enqueueAt,
        object? parameters = null,
        JobOptions? options = null,
        CancellationToken cancellationToken = default) where TJob : IScheduledJob
    {
        var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
        var jobId = GenerateJobId();
        var (jobDetail, trigger) = BuildOneShot<TJob>(jobId, parameters, options,
            b => b.StartAt(enqueueAt));

        await scheduler.ScheduleJob(jobDetail, trigger, default, cancellationToken);

        _logger.LogInformation(
            "Scheduled job [{JobType}] → Quartz key [{JobId}] at [{EnqueueAt}]",
            typeof(TJob).Name, jobId, enqueueAt);

        return jobId;
    }

    /// <inheritdoc />
    /// <remarks>
    /// Quartz.NET does not have native continuation support like Hangfire.
    /// This implementation uses a <see cref="ITriggerListener"/> pattern:
    /// the continuation job is scheduled with a <c>NeverFireTrigger</c>, and a
    /// listener triggers it when the parent completes.
    /// <para>
    /// For simplicity, this throws <see cref="NotSupportedException"/>.
    /// Applications needing continuations should use Hangfire or implement
    /// a custom <see cref="IJobListener"/> via <see cref="QuartzSchedulingOptions.ConfigureQuartz"/>.
    /// </para>
    /// </remarks>
    public Task<string> ContinueWithAsync<TJob>(
        string parentJobId,
        object? parameters = null,
        JobOptions? options = null,
        CancellationToken cancellationToken = default) where TJob : IScheduledJob
    {
        throw new NotSupportedException(
            "Quartz.NET does not natively support job continuations. " +
            "Use Hangfire for continuation workflows, or implement a custom IJobListener.");
    }

    /// <inheritdoc />
    public async Task<bool> CancelAsync(string jobId, CancellationToken cancellationToken = default)
    {
        var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
        var jobKey = new JobKey(jobId, DefaultGroup);
        var deleted = await scheduler.DeleteJob(jobKey, cancellationToken);

        _logger.LogInformation(
            "Cancel job [{JobId}] → {Result}",
            jobId, deleted ? "deleted" : "not found");

        return deleted;
    }

    // ─── Internal helpers ────────────────────────────────────────────

    private (IJobDetail, ITrigger) BuildOneShot<TJob>(
        string jobId,
        object? parameters,
        JobOptions? options,
        Action<TriggerBuilder<IJob>> configureTrigger) where TJob : IScheduledJob
    {
        var dataMap = new JobDataMap();
        PopulateDataMap<TJob>(dataMap, parameters, options);

        var jobDetail = JobBuilder.Create<QuartzJobAdapter<TJob>>()
            .WithIdentity(jobId, DefaultGroup)
            .WithDescription(options?.DisplayName ?? typeof(TJob).Name)
            .UsingJobData(dataMap)
            .StoreDurably(false)
            .Build();

        var triggerBuilder = TriggerBuilder.Create()
            .WithIdentity($"{jobId}-trigger", DefaultGroup)
            .ForJob(jobDetail);

        configureTrigger(triggerBuilder);

        return (jobDetail, triggerBuilder.Build());
    }

    private void PopulateDataMap<TJob>(
        JobDataMap dataMap, object? parameters, JobOptions? options) where TJob : IScheduledJob
    {
        dataMap[QuartzDataMapKeys.JobTypeName] = typeof(TJob).AssemblyQualifiedName!;
        dataMap[QuartzDataMapKeys.MaxRetries] = options?.MaxRetries ?? _options.DefaultMaxRetries;
        dataMap[QuartzDataMapKeys.RetryAttempt] = 0;

        if (parameters is not null)
            dataMap[QuartzDataMapKeys.SerializedParameters] =
                JsonSerializer.Serialize(parameters, parameters.GetType(), JsonOptions);

        if (options is null) return;

        if (!string.IsNullOrEmpty(options.TenantId))
            dataMap[QuartzDataMapKeys.TenantId] = options.TenantId;
        if (!string.IsNullOrEmpty(options.CorrelationId))
            dataMap[QuartzDataMapKeys.CorrelationId] = options.CorrelationId;
        if (!string.IsNullOrEmpty(options.UserId))
            dataMap[QuartzDataMapKeys.UserId] = options.UserId;

        foreach (var kvp in options.Metadata)
            dataMap[$"meta.{kvp.Key}"] = kvp.Value;
    }

    private static string GenerateJobId() => $"lha-{Guid.NewGuid():N}";
}
