using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Quartz;

namespace LHA.BackgroundJob.Quartz;

/// <summary>
/// <see cref="IBackgroundJobManager"/> implementation that delegates to Quartz.NET.
/// Enqueued jobs are scheduled as Quartz trigger-once jobs executed
/// by <see cref="QuartzJobExecutionAdapter{TArgs}"/>.
/// </summary>
public sealed class QuartzBackgroundJobManager : IBackgroundJobManager
{
    private readonly ISchedulerFactory _schedulerFactory;
    private readonly IBackgroundJobSerializer _serializer;
    private readonly BackgroundJobOptions _jobOptions;
    private readonly QuartzBackgroundJobOptions _quartzOptions;
    private readonly ILogger<QuartzBackgroundJobManager> _logger;

    public QuartzBackgroundJobManager(
        ISchedulerFactory schedulerFactory,
        IBackgroundJobSerializer serializer,
        IOptions<BackgroundJobOptions> jobOptions,
        IOptions<QuartzBackgroundJobOptions> quartzOptions,
        ILogger<QuartzBackgroundJobManager> logger)
    {
        _schedulerFactory = schedulerFactory;
        _serializer = serializer;
        _jobOptions = jobOptions.Value;
        _quartzOptions = quartzOptions.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<string> EnqueueAsync<TArgs>(
        TArgs args,
        CBackgroundJobPriority priority = CBackgroundJobPriority.Normal,
        TimeSpan? delay = null)
    {
        ArgumentNullException.ThrowIfNull(args);

        var config = _jobOptions.GetJob(typeof(TArgs));
        var serializedArgs = _serializer.Serialize(args);

        var jobId = $"{config.JobName}-{Guid.CreateVersion7():N}";
        var adapterType = typeof(QuartzJobExecutionAdapter<>).MakeGenericType(typeof(TArgs));

        var jobDetail = JobBuilder.Create()
            .OfType(adapterType)
            .WithIdentity(jobId, _quartzOptions.JobGroup)
            .UsingJobData(QuartzBackgroundJobDataKeys.SerializedArgs, serializedArgs)
            .UsingJobData(QuartzBackgroundJobDataKeys.JobName, config.JobName)
            .StoreDurably(false)
            .RequestRecovery(_quartzOptions.RequestRecovery)
            .Build();

        var triggerBuilder = TriggerBuilder.Create()
            .WithIdentity($"{jobId}-trigger", _quartzOptions.TriggerGroup)
            .WithPriority(MapPriority(priority))
            .ForJob(jobDetail);

        if (delay.HasValue && delay.Value > TimeSpan.Zero)
        {
            triggerBuilder.StartAt(DateTimeOffset.UtcNow.Add(delay.Value));
        }
        else
        {
            triggerBuilder.StartNow();
        }

        var trigger = triggerBuilder.Build();

        var scheduler = await _schedulerFactory.GetScheduler();
        await scheduler.ScheduleJob(jobDetail, trigger, default, CancellationToken.None);

        _logger.LogInformation(
            "Enqueued background job {JobName} via Quartz. JobId: {JobId}, Delay: {Delay}.",
            config.JobName, jobId, delay);

        return jobId;
    }

    /// <summary>
    /// Maps <see cref="CBackgroundJobPriority"/> to a Quartz trigger priority (int).
    /// Default Quartz priority is 5.
    /// </summary>
    private static int MapPriority(CBackgroundJobPriority priority) => priority switch
    {
        CBackgroundJobPriority.High => 10,
        CBackgroundJobPriority.AboveNormal => 8,
        CBackgroundJobPriority.Normal => 5,
        CBackgroundJobPriority.BelowNormal => 3,
        CBackgroundJobPriority.Low => 1,
        _ => 5
    };
}
