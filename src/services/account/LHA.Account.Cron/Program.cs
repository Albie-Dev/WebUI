using Hangfire.PostgreSql;
using LHA.Account.Cron.Workers;
using LHA.Account.EntityFrameworkCore;
using LHA.Auditing;
using LHA.BackgroundWorker.Hangfire;
using LHA.MultiTenancy;
using LHA.Scheduling.Hangfire;
using LHA.UnitOfWork;

var builder = Host.CreateApplicationBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("Default")
    ?? throw new InvalidOperationException("Missing 'Default' connection string.");

// ── Framework services ──────────────────────────────────────────
builder.Services.AddLHAAuditLogging();
builder.Services.AddLHAMultiTenancy();
builder.Services.AddLHAUnitOfWork();
// ── EF Core ─────────────────────────────────────────────────────
builder.Services.AddAccountEntityFrameworkCore(connectionString);

// ── Scheduled jobs ──────────────────────────────────────────────
builder.Services.AddLHAHangfireScheduling(options =>
{
    options.ConfigureHangfire = config =>
        config.UsePostgreSqlStorage(connectionString);
});

builder.Services.AddLHAHangfireBackgroundWorker<SendBirthdayEmailWorker>();

var host = builder.Build();
host.Run();
