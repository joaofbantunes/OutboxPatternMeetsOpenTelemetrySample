using System.Net.Mime;
using System.Text.Json;
using RabbitMQ.Client;

namespace Producer;

internal class EventPublisher
{
    private readonly ILogger<EventPublisher> _logger;
    private readonly IModel _model;

    public EventPublisher(IConnection connection, ILogger<EventPublisher> logger)
    {
        _logger = logger;
        _model = connection.CreateModel();
        _model.ExchangeDeclare("sample.exchange", ExchangeType.Topic);
    }

    public void Publish(Event @event)
    {
        using var activity = EventPublisherActivitySource.StartActivity(
            "sample.exchange",
            @event);

        var properties = _model.CreateBasicProperties();
        properties.ContentType = MediaTypeNames.Application.Json;
        properties.DeliveryMode = 2;
        properties.Headers = EventPublisherActivitySource.EnrichHeadersWithTracingContext(
            activity,
            new Dictionary<string, object>());
        _model.BasicPublish(
            "sample.exchange",
            "sample.key",
            properties,
            JsonSerializer.SerializeToUtf8Bytes(@event));
        _logger.LogInformation("Published event: {Event}", @event);
    }
}