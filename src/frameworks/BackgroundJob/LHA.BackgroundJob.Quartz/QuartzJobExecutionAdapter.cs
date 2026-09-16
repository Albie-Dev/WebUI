using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Quartz;

namespace LHA.BackgroundJob.Quartz;

/// <summary>
/// Quartz <see cref="IJob"/> adapter that bridges Quartz.NET's execution to the LHA background job system.
/// One instance per job args type: <c>QuartzJobExecutionAdapter&lt;TArgs&gt;</c>.
/// Quartz triggers this adapter, which deserializes the args from <see cref="JobDataMap"/>
/// and delegates to <see cref="IBackgroundJobExecuter"/>.
/// </summary>
/// <typeparam name="TArgs">The background job arguments type.</typeparam>
[DisallowConcurrentExecution]
public sealed class QuartzJobExecutionAdapter<TArgs> : IJob
{
    private readonly IBackgroundJobExecuter _executer;
    private readonly IBackgroundJobSerializer _serializer;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly BackgroundJobOptions _options;
    private readonly ILogger<QuartzJobExecutionAdapter<TArgs>> _logger;

    public QuartzJobExecutionAdapter(
        IBackgroundJobExecuter executer,
        IBackgroundJobSerializer serializer,
        IServiceScopeFactory scopeFactory,
        IOptions<BackgroundJobOptions> options,
        ILogger<QuartzJobExecutionAdapter<TArgs>> logger)
    {
        _executer = executer;
        _serializer = serializer;
        _scopeFactory = scopeFactory;
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>
    /// Invoked by Quartz. Extracts serialized args from <see cref="JobDataMap"/> and delegates to the executer.
    /// </summary>
    public async ValueTask Execute(
        IJobExecutionContext quartzContext,
        CancellationToken cancellationToken = default)
    {
        if (!_options.IsJobExecutionEnabled)
        {
            _logger.LogWarning(
                "Background job execution is disabled. Skipping job with args type {ArgsType}.",
                typeof(TArgs).Name);
            return;
        }

        var dataMap = quartzContext.MergedJobDataMap;
        var serializedArgs = dataMap.GetString(QuartzBackgroundJobDataKeys.SerializedArgs)
            ?? throw new JobExecutionException(
                $"Missing '{QuartzBackgroundJobDataKeys.SerializedArgs}' in JobDataMap for {typeof(TArgs).Name}.");

        var argsType = typeof(TArgs);
        var config = _options.GetJob(argsType);
        var args = _serializer.Deserialize(serializedArgs, argsType);

        await using var scope = _scopeFactory.CreateAsyncScope();

        var context = new JobExecutionContext
        {
            ServiceProvider = scope.ServiceProvider,
            JobType = config.JobType,
            ArgsType = argsType,
            JobArgs = args,
            CancellationToken = cancellationToken
        };

        _logger.LogDebug(
            "Quartz executing background job {JobType} with args type {ArgsType}. Fire instance: {FireId}.",
            config.JobType.Name, argsType.Name, quartzContext.FireInstanceId);

        try
        {
            await _executer.ExecuteAsync(context);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Background job {JobType} failed in Quartz adapter.", config.JobType.Name);
            throw new JobExecutionException(ex);
        }
    }
}

/// <summary>
/// Well-known keys used in the Quartz <see cref="JobDataMap"/> for background job data.
/// </summary>
public static class QuartzBackgroundJobDataKeys
{
    /// <summary>Key for the JSON-serialized job arguments.</summary>
    public const string SerializedArgs = "lha.bg.args";

    /// <summary>Key for the job name (used for lookup in <see cref="BackgroundJobOptions"/>).</summary>
    public const string JobName = "lha.bg.jobName";
}
