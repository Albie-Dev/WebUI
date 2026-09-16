using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Quartz;

namespace LHA.BackgroundWorker.Quartz;

/// <summary>
/// Base class for background workers scheduled by Quartz.NET.
/// Provides persistent, cluster-safe periodic execution with cron-based scheduling
/// and misfire handling — replacing the simple timer-based <c>PeriodicBackgroundWorker</c>.
/// </summary>
/// <remarks>
/// On <see cref="StartAsync"/>, the worker registers a Quartz trigger with the
/// specified <see cref="CronExpression"/>. When Quartz fires the trigger,
/// <see cref="QuartzWorkerJobAdapter"/> resolves this worker from DI and
/// calls <see cref="DoWorkAsync"/>. On <see cref="StopAsync"/>, the trigger is removed.
/// </remarks>
/// <example>
/// <code>
/// public sealed class CleanupWorker : QuartzPeriodicBackgroundWorker
/// {
///     protected override string CronExpression => CronPresets.EveryMinute;
///
///     public CleanupWorker(ISchedulerFactory sf, ILogger&lt;CleanupWorker&gt; logger)
///         : base(sf, logger) { }
///
///     public override async Task DoWorkAsync(CancellationToken cancellationToken)
///     {
///         // periodic cleanup logic
///     }
/// }
///
/// // Registration:
/// services.AddLHAQuartzBackgroundWorker&lt;CleanupWorker&gt;();
/// </code>
/// </example>
public abstract class QuartzPeriodicBackgroundWorker : IBackgroundWorker, IDisposable
{
    private readonly ISchedulerFactory _schedulerFactory;

    /// <summary>Logger for the worker.</summary>
    protected ILogger Logger { get; }

    /// <summary>
    /// Cron expression defining the execution schedule.
    /// Use <c>LHA.Scheduling.CronPresets</c> for common patterns.
    /// </summary>
    protected abstract string CronExpression { get; }

    /// <summary>
    /// Quartz job/trigger group name. Default: <c>"LHA.BackgroundWorkers"</c>.
    /// </summary>
    protected virtual string Group => "LHA.BackgroundWorkers";

    /// <summary>
    /// Creates a new Quartz-based periodic background worker.
    /// </summary>
    /// <param name="schedulerFactory">Quartz scheduler factory (injected by DI).</param>
    /// <param name="logger">Optional logger; falls back to <see cref="NullLogger"/>.</param>
    protected QuartzPeriodicBackgroundWorker(
        ISchedulerFactory schedulerFactory,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(schedulerFactory);
        _schedulerFactory = schedulerFactory;
        Logger = logger ?? NullLogger.Instance;
    }

    /// <inheritdoc />
    public virtual async Task StartAsync(CancellationToken cancellationToken = default)
    {
        var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
        var workerTypeName = GetType().FullName!;
        var jobKey = new JobKey(workerTypeName, Group);

        // Remove stale job from a previous run (e.g. when using a persistent store)
        if (await scheduler.Exists(jobKey, cancellationToken))
        {
            await scheduler.DeleteJob(jobKey, cancellationToken);
        }

        var jobDetail = JobBuilder.Create<QuartzWorkerJobAdapter>()
            .WithIdentity(jobKey)
            .UsingJobData(QuartzWorkerJobAdapter.WorkerTypeKey, GetType().AssemblyQualifiedName!)
            .StoreDurably(false)
            .Build();

        var trigger = TriggerBuilder.Create()
            .WithIdentity($"{workerTypeName}.trigger", Group)
            .WithCronSchedule(CronExpression, x =>
                x.WithMisfireInstruction(CronTriggerMisfireInstruction.FireAndProceed))
            .StartNow()
            .Build();

        await scheduler.ScheduleJob(jobDetail, trigger, default, cancellationToken);

        Logger.LogInformation(
            "Scheduled Quartz background worker {WorkerType} with cron '{CronExpression}'.",
            GetType().Name, CronExpression);
    }

    /// <inheritdoc />
    public virtual async Task StopAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
            var jobKey = new JobKey(GetType().FullName!, Group);

            if (await scheduler.Exists(jobKey, cancellationToken))
            {
                await scheduler.DeleteJob(jobKey, cancellationToken);
            }

            Logger.LogDebug("Unscheduled Quartz background worker {WorkerType}.", GetType().Name);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Logger.LogWarning(ex,
                "Failed to cleanly unschedule Quartz background worker {WorkerType}.",
                GetType().Name);
        }
    }

    /// <summary>
    /// Override to implement the periodic work logic.
    /// Called each time the Quartz trigger fires.
    /// </summary>
    /// <param name="cancellationToken">Token signaled when the job should abort.</param>
    public abstract Task DoWorkAsync(CancellationToken cancellationToken);

    /// <inheritdoc />
    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }
}
