using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Quartz;

namespace LHA.Scheduling.Quartz;

/// <summary>
/// Extension methods for registering the LHA Quartz Scheduling framework
/// in the dependency injection container.
/// </summary>
public static class QuartzServiceCollectionExtensions
{
    /// <summary>
    /// Adds LHA Scheduling services backed by Quartz.NET to the service collection.
    /// Registers <see cref="IJobScheduler"/> and <see cref="IRecurringJobManager"/> implementations.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">
    /// Configure Quartz scheduling options. Use <see cref="QuartzSchedulingOptions.ConfigureQuartz"/>
    /// for advanced Quartz-specific settings (e.g. persistent stores, cluster mode).
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    /// <example>
    /// <code>
    /// services.AddLHAQuartzScheduling(options =>
    /// {
    ///     options.ThreadCount = 5;
    ///     options.SchedulerName = "MyAppScheduler";
    ///     options.ConfigureQuartz = q =>
    ///     {
    ///         q.UsePersistentStore(store =>
    ///         {
    ///             store.UsePostgres("Host=localhost;Database=quartz;...");
    ///             store.UseNewtonsoftJsonSerializer();
    ///         });
    ///     };
    /// });
    /// services.AddScheduledJob&lt;CleanupExpiredOrdersJob&gt;();
    /// </code>
    /// </example>
    public static IServiceCollection AddLHAQuartzScheduling(
        this IServiceCollection services,
        Action<QuartzSchedulingOptions> configure)
    {
        var options = new QuartzSchedulingOptions();
        configure(options);

        services.Configure(configure);

        // Register Quartz.NET core services
        services.AddQuartz(q =>
        {
            q.ConfigureScheduler(scheduler =>
            {
                scheduler.InstanceName = options.SchedulerName;
            });

            // Thread pool
            q.UseDefaultThreadPool(tp =>
            {
                tp.MaxConcurrency = options.ThreadCount;
            });

            // Persistent store (optional)
            if (options.UsePersistentStore)
            {
                if (string.IsNullOrWhiteSpace(options.ConnectionString))
                    throw new InvalidOperationException(
                        "ConnectionString is required when UsePersistentStore is enabled.");

                if (string.IsNullOrWhiteSpace(options.AdoNetProvider))
                    throw new InvalidOperationException(
                        "AdoNetProvider is required when UsePersistentStore is enabled. " +
                        "Examples: \"Npgsql\", \"SqlServer\", \"MySql\".");

                var connectionString = options.ConnectionString;
                var tablePrefix = options.TablePrefix;

                q.UsePersistentStore(store =>
                {
                    store.ConfigureStore(jobStore =>
                    {
                        jobStore.StoreJobDataAsStrings = true;
                        jobStore.DbRetryInterval = TimeSpan.FromSeconds(15);
                        jobStore.SchemaProvisioning = SchemaProvisioning.Validate;
                        jobStore.TablePrefix = tablePrefix;
                        jobStore.MisfireThreshold = TimeSpan.FromMilliseconds(options.MisfireThresholdMs);
                    });

                    store.UseGenericDatabase(options.AdoNetProvider, connectionString);
                });
            }
            else
            {
                q.UseInMemoryStore(store =>
                {
                    store.MisfireThreshold = TimeSpan.FromMilliseconds(options.MisfireThresholdMs);
                });
            }

            // Advanced configuration callback
            options.ConfigureQuartz?.Invoke(q);
        });

        // Add Quartz hosted service (scheduler lifecycle management)
        services.AddQuartzHostedService(hostOptions =>
        {
            hostOptions.WaitForJobsToComplete = options.WaitForJobsToComplete;
        });

        // Register LHA abstractions → Quartz implementations
        services.TryAddSingleton<IJobScheduler, QuartzJobScheduler>();
        services.TryAddSingleton<IRecurringJobManager, QuartzRecurringJobManager>();
        services.TryAddSingleton<ISchedulingHealthCheck, QuartzHealthCheck>();

        return services;
    }

    /// <summary>
    /// Registers an <see cref="IScheduledJob"/> implementation in DI.
    /// The job will be resolved from a scope when executed by the Quartz adapter.
    /// </summary>
    /// <typeparam name="TJob">The job implementation type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddScheduledJob<TJob>(this IServiceCollection services)
        where TJob : class, IScheduledJob
    {
        services.TryAddScoped<TJob>();
        return services;
    }
}
