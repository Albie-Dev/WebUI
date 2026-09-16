using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Quartz;

namespace LHA.Scheduling.Quartz;

/// <summary>
/// Quartz.NET implementation of <see cref="IRecurringJobManager"/>.
/// Maps recurring job registrations to Quartz cron triggers.
/// </summary>
/// <remarks>
/// Recurring jobs use a dedicated trigger group <c>"lha-recurring"</c>.
/// Each recurring job has a <see cref="JobKey"/> and <see cref="TriggerKey"/>
/// derived from the <c>recurringJobId</c>.
/// </remarks>
public sealed class QuartzRecurringJobManager : IRecurringJobManager
{
    private readonly ISchedulerFactory _schedulerFactory;
    private readonly QuartzSchedulingOptions _options;
    private readonly ILogger<QuartzRecurringJobManager> _logger;

    private const string RecurringGroup = "lha-recurring";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        WriteIndented = false,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };

    public QuartzRecurringJobManager(
        ISchedulerFactory schedulerFactory,
        IOptions<QuartzSchedulingOptions> options,
        ILogger<QuartzRecurringJobManager> logger)
    {
        _schedulerFactory = schedulerFactory;
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task AddOrUpdateAsync<TJob>(
        string recurringJobId,
        string cronExpression,
        RecurringJobOptions? options = null,
        CancellationToken cancellationToken = default) where TJob : IScheduledJob
    {
        var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
        var jobKey = new JobKey(recurringJobId, RecurringGroup);
        var triggerKey = new TriggerKey(recurringJobId, RecurringGroup);

        var timeZone = TimeZoneInfo.FindSystemTimeZoneById(options?.TimeZoneId ?? "UTC");

        var dataMap = new JobDataMap();
        dataMap[QuartzDataMapKeys.JobTypeName] = typeof(TJob).AssemblyQualifiedName ?? typeof(TJob).FullName ?? "unknown";
        dataMap[QuartzDataMapKeys.MaxRetries] = options?.MaxRetries ?? _options.DefaultMaxRetries;
        dataMap[QuartzDataMapKeys.RetryAttempt] = 0;

        if (options?.Parameters is not null)
            dataMap[QuartzDataMapKeys.SerializedParameters] =
                JsonSerializer.Serialize(options.Parameters, options.Parameters.GetType(), JsonOptions);

        if (!string.IsNullOrEmpty(options?.TenantId))
            dataMap[QuartzDataMapKeys.TenantId] = options.TenantId;

        foreach (var kvp in options?.Metadata ?? new Dictionary<string, string>())
            dataMap[$"meta.{kvp.Key}"] = kvp.Value;

        var jobDetail = JobBuilder.Create<QuartzJobAdapter<TJob>>()
            .WithIdentity(jobKey)
            .WithDescription(options?.DisplayName ?? recurringJobId)
            .UsingJobData(dataMap)
            .StoreDurably(true) // Keep even when no triggers (allows TriggerAsync)
            .Build();

        var triggerBuilder = TriggerBuilder.Create()
            .WithIdentity(triggerKey)
            .ForJob(jobDetail)
            .WithCronSchedule(cronExpression, cron =>
            {
                cron.InTimeZone(timeZone);
                ApplyMisfirePolicy(cron, options?.MisfirePolicy ?? MisfirePolicy.FireOnceNow);
            });

        var trigger = triggerBuilder.Build();

        // Check if exists → reschedule; otherwise → schedule new
        if (await scheduler.Exists(jobKey, cancellationToken))
        {
            await scheduler.AddJob(jobDetail, AddJobOptions.Replacing, cancellationToken);
            await scheduler.RescheduleJob(triggerKey, trigger, cancellationToken);

            _logger.LogInformation(
                "Updated recurring job [{RecurringJobId}] cron [{Cron}] tz [{TimeZone}]",
                recurringJobId, cronExpression, timeZone.Id);
        }
        else
        {
            await scheduler.ScheduleJob(jobDetail, trigger, default, cancellationToken);

            _logger.LogInformation(
                "Created recurring job [{RecurringJobId}] cron [{Cron}] tz [{TimeZone}]",
                recurringJobId, cronExpression, timeZone.Id);
        }
    }

    /// <inheritdoc />
    public async Task RemoveAsync(string recurringJobId, CancellationToken cancellationToken = default)
    {
        var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
        var jobKey = new JobKey(recurringJobId, RecurringGroup);
        var deleted = await scheduler.DeleteJob(jobKey, cancellationToken);

        _logger.LogInformation(
            "Remove recurring job [{RecurringJobId}] → {Result}",
            recurringJobId, deleted ? "deleted" : "not found");
    }

    /// <inheritdoc />
    public async Task TriggerAsync(string recurringJobId, CancellationToken cancellationToken = default)
    {
        var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
        var jobKey = new JobKey(recurringJobId, RecurringGroup);

        if (!await scheduler.Exists(jobKey, cancellationToken))
            throw new InvalidOperationException($"Recurring job '{recurringJobId}' does not exist.");

        await scheduler.TriggerJob(jobKey, null, cancellationToken);

        _logger.LogInformation("Triggered recurring job [{RecurringJobId}]", recurringJobId);
    }

    /// <inheritdoc />
    public async Task<bool> ExistsAsync(string recurringJobId, CancellationToken cancellationToken = default)
    {
        var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
        return await scheduler.Exists(new JobKey(recurringJobId, RecurringGroup), cancellationToken);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<RecurringJobDefinition>> ListAsync(CancellationToken cancellationToken = default)
    {
        var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
        var jobKeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(RecurringGroup), cancellationToken);
        var result = new List<RecurringJobDefinition>();

        foreach (var jobKey in jobKeys)
        {
            var jobDetail = await scheduler.GetJobDetail(jobKey, cancellationToken);
            if (jobDetail is null) continue;

            var triggers = await scheduler.GetTriggersOfJob(jobKey, cancellationToken);
            var cronTrigger = triggers.OfType<ICronTrigger>().FirstOrDefault();

            var status = RecurringJobStatus.Active;
            DateTimeOffset? nextFire = null;
            DateTimeOffset? prevFire = null;
            var cron = string.Empty;
            var timeZoneId = "UTC";

            if (cronTrigger is not null)
            {
                cron = cronTrigger.CronExpressionString ?? string.Empty;
                nextFire = cronTrigger.NextFireTimeUtc;
                prevFire = cronTrigger.PreviousFireTimeUtc;
                timeZoneId = cronTrigger.TimeZone.Id;

                var triggerState = await scheduler.GetTriggerState(cronTrigger.Key, cancellationToken);
                status = triggerState switch
                {
                    TriggerState.Paused => RecurringJobStatus.Paused,
                    TriggerState.None => RecurringJobStatus.Removed,
                    _ => RecurringJobStatus.Active
                };
            }

            result.Add(new RecurringJobDefinition
            {
                RecurringJobId = jobKey.Name,
                CronExpression = cron,
                JobType = jobDetail.JobDataMap.GetString(QuartzDataMapKeys.JobTypeName) ?? jobDetail.JobType.FullName ?? "unknown",
                Queue = null, // Quartz doesn't map directly to queues
                TimeZoneId = timeZoneId,
                LastExecutedAt = prevFire,
                NextExecutionAt = nextFire,
                Status = status
            });
        }

        return result;
    }

    // ─── Internal helpers ────────────────────────────────────────────

    private static void ApplyMisfirePolicy(CronScheduleBuilder cron, MisfirePolicy policy)
    {
        switch (policy)
        {
            case MisfirePolicy.FireOnceNow:
                cron.WithMisfireInstruction(CronTriggerMisfireInstruction.FireAndProceed);
                break;
            case MisfirePolicy.IgnoreMisfire:
                cron.WithMisfireInstruction(CronTriggerMisfireInstruction.IgnoreMisfires);
                break;
            case MisfirePolicy.FireAll:
                // Quartz has no direct "fire all" equivalent;
                // DoNothing waits for next scheduled time (closest safe default)
                cron.WithMisfireInstruction(CronTriggerMisfireInstruction.DoNothing);
                break;
            default:
                cron.WithMisfireInstruction(CronTriggerMisfireInstruction.FireAndProceed);
                break;
        }
    }
}
