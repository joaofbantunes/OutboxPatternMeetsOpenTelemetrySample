using System.Text.Json;
using Consumer;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<IConnection>(_ =>
{
    var factory = new ConnectionFactory { HostName = "localhost" };
    return factory.CreateConnection();
});
builder.Services.AddHostedService<EventConsumer>();
builder.Services.AddOpenTelemetry()
    .ConfigureResource(resourceBuilder =>
        resourceBuilder.AddService(
            serviceName: "consumer",
            serviceVersion: typeof(Program).Assembly.GetName().Version?.ToString() ?? "unknown",
            serviceInstanceId: Environment.MachineName))
    .WithTracing(traceBuilder => traceBuilder
        .AddSource(nameof(EventConsumer))
        .AddAspNetCoreInstrumentation()
        .AddOtlpExporter(options => { options.Endpoint = new Uri("http://localhost:4317"); }));


var app = builder.Build();

app.MapGet("/", () => "Hello World!");

app.Run();

internal record Event(Guid Id, string Text);

internal class EventConsumer(IConnection connection, ILogger<EventConsumer> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var model = connection.CreateModel();
        model.ExchangeDeclare("sample.exchange", ExchangeType.Topic);
        model.QueueDeclare("sample.queue", true, false, false, null);
        model.QueueBind("sample.queue", "sample.exchange", "sample.*");
        var consumer = new EventingBasicConsumer(model);
        consumer.Received += OnReceived;
        var consumerTag = model.BasicConsume("sample.queue", true, consumer);

        var tcs = new TaskCompletionSource();
        await using var _ = stoppingToken.Register(() => tcs.SetResult());
        await tcs.Task;

        model.BasicCancel(consumerTag);
    }

    void OnReceived(object? sender, BasicDeliverEventArgs eventArgs)
    {
        var @event = JsonSerializer.Deserialize<Event>(eventArgs.Body.Span)!;
        using var activity = EventConsumerActivitySource.StartActivity(
            "sample.exchange",
            @event,
            eventArgs.BasicProperties.Headers);

        logger.LogInformation("Received message: {Event}", @event);
    }
}