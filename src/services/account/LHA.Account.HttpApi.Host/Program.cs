using System.Text;
using Hangfire.PostgreSql;
using LHA.Caching;
using LHA.Account.Application;
using LHA.Account.Domain.Shared.Localization;
using LHA.Account.EntityFrameworkCore;
using LHA.Account.HttpApi;
using LHA.AspNetCore;
using LHA.Scheduling.Hangfire;
using LHA.Auditing;
using LHA.Auditing.Interceptors;
using LHA.DistributedLocking;
using LHA.Grpc.Server;
using LHA.Identity.Domain.Shared.Localization;
using LHA.MultiTenancy;
using LHA.Swagger;
using LHA.UnitOfWork;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.EntityFrameworkCore;
using Microsoft.IdentityModel.Tokens;
using LHA.Shared.Contracts;
using LHA.MessageBroker.Kafka;
using LHA.EventBus.Kafka;
using LHA.AspNetCore.Security;

var builder = WebApplication.CreateBuilder(args);

// ── Framework services ───────────────────────────────────────────
builder.Services.AddLHAMultiTenancyHosting(options =>
{
    options.TenantResolvers.Add(new LHA.AspNetCore.Security.DomainTenantResolveContributor());
    options.TenantResolvers.Add(new LHA.AspNetCore.Security.HttpHeaderTenantResolveContributor());
});
builder.Services.AddLHAUnitOfWork();
builder.Services.AddLHADistributedLocking();
builder.Services.AddLHACaching();

// ── Kafka infrastructure ────────────────────────────────────────
builder.Services.AddLHAKafka(kafka =>
{
    kafka.BootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
});

// ── Kafka event bus (publisher mode) ────────────────────────────
builder.Services.AddLHAKafkaEventBus(
    eventBus =>
    {
        eventBus.ConsumerGroup = "account-service";
        eventBus.ApplicationName = "AccountHttpApi";
        eventBus.EnableOutbox = true;
    },
    kafka =>
    {
        kafka.DefaultTopic = LHA.Shared.Contracts.EventTopics.AccountEvents;
    });

builder.Services.AddKafkaOutboxProcessor();

// ── Module services (Application + EF Core) ──────────────────────
var connectionString = builder.Configuration.GetConnectionString("Default")
    ?? throw new InvalidOperationException("Missing 'Default' connection string.");

builder.Services.AddAccountApplication();
builder.Services.AddAccountEntityFrameworkCore(connectionString);

builder.Services.AddLHAHangfireScheduling(options =>
{
    options.ConfigureHangfire = config =>
        config.UsePostgreSqlStorage(connectionString);
    options.EnableServer = false; // API node only enqueues and monitors, does not process jobs
});

// ─── AUDIT LOG PRODUCER ───
// Use AuditingMode to control which audit producers run in this App.
// Make sure it matches the storage setup (CAuditLogStoreMode) in Account.EntityFrameworkCore!
builder.Services.AddLHAAuditLogging(
    mode: CAuditingMode.All,
    configureDataAudit: options =>
    {
        options.ApplicationName = "Account";
        options.CaptureRequestBody = true;
    },
    configurePipeline: options =>
    {
        options.ServiceName = "Account";
    });

// ── Swagger / OpenAPI ─────────────────────────────────────────────
builder.Services.AddLhaApiVersioning();
builder.Services.AddLHASwagger(builder.Configuration);

// ── Global exception handler ──────────────────────────────────────
builder.Services.AddLHAAspNetCore(typeof(AccountResource), typeof(IdentityResource));

// ── JWT configuration ────────────────────────────────────────────
builder.Services.Configure<JwtOptions>(builder.Configuration.GetSection(JwtOptions.SectionName));
builder.Services.AddLHASecurityVersioning(builder.Configuration);

var jwtSection = builder.Configuration.GetSection(JwtOptions.SectionName);
var secretKey = jwtSection["SecretKey"] ?? throw new InvalidOperationException("Missing JWT SecretKey.");

builder.Services
    .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        options.TokenValidationParameters = new TokenValidationParameters
        {
            ValidateIssuer = true,
            ValidIssuer = jwtSection["Issuer"],
            ValidateAudience = true,
            ValidAudience = jwtSection["Audience"],
            ValidateLifetime = true,
            ValidateIssuerSigningKey = true,
            IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(secretKey)),
            ClockSkew = TimeSpan.FromMinutes(1),
        };
        options.EnableSecurityVersionValidation();
    });

builder.Services.AddLHAPermissionAuthorization();

// ── gRPC server ───────────────────────────────────────────────────
builder.Services.AddLHAGrpcServer();
builder.Services.AddGrpc(options =>
{
    options.Interceptors.Add<AuditGrpcInterceptor>();
});

var app = builder.Build();

// ── Middleware ────────────────────────────────────────────────────
app.UseLHAAspNetCore();

app.UseLHASwagger();
app.UseAuthentication();
app.UseJwtTenantResolve(); // Resolve tenant from JWT tenant_id claim
// Apply chosen Audit Middlewares via the Facade (must match earlier setup)
app.UseLHAAuditLogging(mode: CAuditingMode.DataAudit);
app.UseAuthorization();

// ── Endpoints ────────────────────────────────────────────────────
app.MapAccountEndpoints();
app.MapLHAGrpcInfrastructure();

// ── Auto Migration ───────────────────────────────────────────────
using (var scope = app.Services.CreateScope())
{
    var context = scope.ServiceProvider.GetRequiredService<AccountDbContext>();
    await context.Database.MigrateAsync();
}

app.Run();
