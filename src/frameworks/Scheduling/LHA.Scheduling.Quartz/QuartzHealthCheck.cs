using Microsoft.Extensions.Logging;
using Quartz;

namespace LHA.Scheduling.Quartz;

/// <summary>
/// Health check for Quartz.NET scheduler infrastructure.
/// Verifies that the scheduler is started and reports job/trigger counts.
/// </summary>
public sealed class QuartzHealthCheck : ISchedulingHealthCheck
{
    private readonly ISchedulerFactory _schedulerFactory;
    private readonly ILogger<QuartzHealthCheck> _logger;

    public string SchedulerType => "Quartz";

    public QuartzHealthCheck(
        ISchedulerFactory schedulerFactory,
        ILogger<QuartzHealthCheck> logger)
    {
        _schedulerFactory = schedulerFactory;
        _logger = logger;
    }

    public async Task<SchedulerHealthResult> CheckHealthAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
            var metadata = await scheduler.GetMetadata(cancellationToken);
            var isRunning = metadata.Status == SchedulerStatus.Running;

            var data = new Dictionary<string, object>
            {
                ["schedulerName"] = metadata.SchedulerName,
                ["started"] = isRunning,
                ["status"] = metadata.Status.ToString(),
                ["schedulerInstanceId"] = metadata.SchedulerInstanceId,
                ["threadPoolSize"] = metadata.ThreadPoolSize,
                ["runningSince"] = metadata.RunningSince?.ToString("O") ?? "never",
                ["jobStoreClustered"] = metadata.JobStoreClustered,
                ["jobStoreSupportsPersistence"] = metadata.JobStorePersistent,
                ["numberOfJobsExecuted"] = metadata.JobsExecuted
            };

            if (!isRunning)
            {
                return new SchedulerHealthResult
                {
                    IsHealthy = false,
                    Description = metadata.Status == SchedulerStatus.Standby
                        ? "Quartz scheduler is in standby mode — jobs will not execute"
                        : $"Quartz scheduler is not running ({metadata.Status})",
                    Data = data
                };
            }

            return new SchedulerHealthResult
            {
                IsHealthy = true,
                Description = $"Quartz healthy: {metadata.SchedulerName}, " +
                              $"pool size {metadata.ThreadPoolSize}, " +
                              $"{metadata.JobsExecuted} jobs executed",
                Data = data
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Quartz health check failed");

            return SchedulerHealthResult.Unhealthy(
                $"Quartz health check exception: {ex.Message}", ex);
        }
    }
}
