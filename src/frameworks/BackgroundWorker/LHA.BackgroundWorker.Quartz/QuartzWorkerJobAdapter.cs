using Microsoft.Extensions.Logging;
using Quartz;

namespace LHA.BackgroundWorker.Quartz;

/// <summary>
/// Quartz <see cref="IJob"/> adapter that bridges Quartz triggers to
/// <see cref="QuartzPeriodicBackgroundWorker.DoWorkAsync"/>.
/// When Quartz fires, this adapter resolves the worker from DI by stored type name
/// and delegates execution.
/// </summary>
[DisallowConcurrentExecution]
internal sealed class QuartzWorkerJobAdapter : IJob
{
    /// <summary>Key used to store the worker's assembly-qualified type name in the Quartz job data map.</summary>
    internal const string WorkerTypeKey = "LHA.BackgroundWorker.WorkerType";

    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<QuartzWorkerJobAdapter> _logger;

    public QuartzWorkerJobAdapter(
        IServiceProvider serviceProvider,
        ILogger<QuartzWorkerJobAdapter> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    public async ValueTask Execute(
        IJobExecutionContext context,
        CancellationToken cancellationToken = default)
    {
        var workerTypeName = context.MergedJobDataMap.GetString(WorkerTypeKey)
            ?? throw new InvalidOperationException(
                "Worker type name not found in Quartz job data map.");

        var workerType = Type.GetType(workerTypeName)
            ?? throw new InvalidOperationException(
                $"Cannot resolve worker type: {workerTypeName}");

        var worker = _serviceProvider.GetService(workerType) as QuartzPeriodicBackgroundWorker
            ?? throw new InvalidOperationException(
                $"Service of type {workerType.Name} is not registered " +
                $"or is not a {nameof(QuartzPeriodicBackgroundWorker)}.");

        _logger.LogDebug("Quartz firing background worker {WorkerType}.", workerType.Name);

        try
        {
            await worker.DoWorkAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Error in Quartz background worker {WorkerType}.", workerType.Name);
            throw new JobExecutionException(ex);
        }
    }
}
