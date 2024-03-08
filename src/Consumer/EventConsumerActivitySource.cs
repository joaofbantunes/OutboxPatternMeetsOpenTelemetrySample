using System.Diagnostics;
using System.Text;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

namespace Consumer;

// note: new versions of RabbitMQ client library will eventually support OpenTelemetry out of the box,
// but this version still needs manual instrumentation

internal static class EventConsumerActivitySource
{
    private const string Name = "event handle";
    private const ActivityKind Kind = ActivityKind.Consumer;
    private const string EventTopicTag = "event.topic";
    private const string EventIdTag = "event.id";
    private const string EventTypeTag = "event.type";

    private static readonly ActivitySource ActivitySource
        = new(nameof(EventConsumer));

    private static readonly TextMapPropagator Propagator
        = Propagators.DefaultTextMapPropagator;

    public static Activity? StartActivity(
        string topic,
        Event @event,
        IDictionary<string, object>? headers)
    {
        if (!ActivitySource.HasListeners())
        {
            return null;
        }

        // Extract the context injected in the headers by the publisher

        var parentContext = Propagator.Extract(
            default,
            headers,
            ExtractTraceContext);

        // Inject extracted info into current context
        Baggage.Current = parentContext.Baggage;

        return ActivitySource.StartActivity(
            Name,
            Kind,
            parentContext.ActivityContext,
            tags: new KeyValuePair<string, object?>[]
            {
                new(EventTopicTag, topic),
                new(EventIdTag, @event.Id),
                new(EventTypeTag, @event.GetType().Name),
            });

        static IEnumerable<string> ExtractTraceContext(
            IDictionary<string, object>? headers,
            string key)
        {
            if (headers is not null && headers.TryGetValue(key, out var value))
            {
                return new[] { Encoding.UTF8.GetString((byte[])value) };
            }

            return Enumerable.Empty<string>();
        }
    }
}