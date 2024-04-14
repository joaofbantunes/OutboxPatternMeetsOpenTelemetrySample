using System.Text.Json;
using Bogus;
using Npgsql;
using NpgsqlTypes;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Producer;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<IConnection>(_ =>
{
    var factory = new ConnectionFactory { HostName = "localhost" };
    return factory.CreateConnection();
});
builder.Services.AddNpgsqlDataSource("server=localhost;port=5432;user id=user;password=pass;database=SampleDb");
builder.Services.AddHostedService<DatabaseInitializer>();
builder.Services.AddHostedService<OutboxPublisher>();
builder.Services.AddSingleton<OutboxListener>();
builder.Services.AddSingleton<EventPublisher>();
builder.Services.AddSingleton<Faker>();
builder.Services.AddOpenTelemetry()
    .ConfigureResource(resourceBuilder =>
        resourceBuilder.AddService(
            serviceName: "producer",
            serviceVersion: typeof(Program).Assembly.GetName().Version?.ToString() ?? "unknown",
            serviceInstanceId: Environment.MachineName))
    .WithTracing(traceBuilder =>
        traceBuilder
            .AddSource(EventPublisherActivitySource.ActivitySourceName)
            .AddSource(OutboxPublisherActivitySource.ActivitySourceName)
            .AddAspNetCoreInstrumentation()
            .AddNpgsql()
            .AddOtlpExporter(options => { options.Endpoint = new Uri("http://localhost:4317"); }));

var app = builder.Build();

app.MapGet("/", () => "Hello World!");
app.MapPost("/do-stuff-without-outbox", (EventPublisher publisher, Faker faker) =>
{
    publisher.Publish(new Event(Guid.NewGuid(), faker.Hacker.Verb()));
    return Results.Accepted();
});

app.MapPost("/do-stuff-with-tracing-disconnected-outbox",
    async (NpgsqlDataSource dataSource,
        Faker faker,
        OutboxListener outboxListener) =>
    {
        var @event = new Event(Guid.NewGuid(), faker.Hacker.Verb());
        await using var command = dataSource.CreateCommand();
        // lang=postgresql
        command.CommandText = "INSERT INTO outbox (payload) VALUES (@Payload);";
        command.Parameters.AddWithValue(
            "Payload",
            NpgsqlDbType.Jsonb,
            JsonSerializer.Serialize(@event));
        _ = await command.ExecuteNonQueryAsync();
        outboxListener.OnNewMessages();
        return Results.Accepted();
    });

app.MapPost("/do-stuff-with-tracing-connected-outbox",
    async (NpgsqlDataSource dataSource, Faker faker, OutboxListener outboxListener) =>
    {
        var @event = new Event(Guid.NewGuid(), faker.Hacker.Verb());
        var telemetryContext = OutboxPublisherActivitySource.ExtractTelemetryContextForPersistence();
        await using var command = dataSource.CreateCommand();
        command.CommandText =
            // lang=postgresql
            """
            INSERT INTO outbox (payload, telemetry_context)
            VALUES (@Payload, @TelemetryContext);
            """;
        command.Parameters.AddWithValue("Payload", NpgsqlDbType.Jsonb, JsonSerializer.Serialize(@event));
        command.Parameters.AddWithValue("TelemetryContext", NpgsqlDbType.Jsonb, telemetryContext);
        _ = await command.ExecuteNonQueryAsync();
        outboxListener.OnNewMessages();
        return Results.Accepted();
    });

app.Run();

internal record Event(Guid Id, string Text);