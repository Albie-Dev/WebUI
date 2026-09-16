namespace LHA.Scheduling.Quartz;

/// <summary>
/// Configuration options for the LHA Quartz.NET Scheduling integration.
/// </summary>
public sealed class QuartzSchedulingOptions
{
    /// <summary>Quartz scheduler instance name.</summary>
    public string SchedulerName { get; set; } = "LHAScheduler";

    /// <summary>
    /// Default max retry attempts for failed jobs.
    /// </summary>
    public int DefaultMaxRetries { get; set; } = 3;

    /// <summary>
    /// If true, the scheduler waits for running jobs to complete before shutting down.
    /// </summary>
    public bool WaitForJobsToComplete { get; set; } = true;

    /// <summary>
    /// Thread pool size for job execution.
    /// </summary>
    public int ThreadCount { get; set; } = 10;

    /// <summary>
    /// If true, uses durable job store (e.g. AdoJobStore for database persistence).
    /// If false, uses RAMJobStore (suitable for single-node or testing).
    /// </summary>
    public bool UsePersistentStore { get; set; } = false;

    /// <summary>
    /// Connection string for the persistent job store.
    /// Required when <see cref="UsePersistentStore"/> is true.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// ADO.NET provider for the persistent job store (e.g. "Npgsql", "SqlServer").
    /// Required when <see cref="UsePersistentStore"/> is true.
    /// </summary>
    public string? AdoNetProvider { get; set; }

    /// <summary>
    /// Table prefix for the Quartz tables in the persistent store.
    /// </summary>
    public string TablePrefix { get; set; } = "QRTZ_";

    /// <summary>
    /// Callback for advanced Quartz configuration beyond what these options cover.
    /// </summary>
    public Action<global::Quartz.IQuartzBuilder>? ConfigureQuartz { get; set; }

    /// <summary>
    /// Misfire threshold in milliseconds.
    /// If a trigger's fire time is older than this threshold, it is considered misfired.
    /// </summary>
    public int MisfireThresholdMs { get; set; } = 60000;
}
