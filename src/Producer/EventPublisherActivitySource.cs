using System.Diagnostics;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

namespace Producer;

// note: new versions of RabbitMQ client library will eventually support OpenTelemetry out of the box,
// but this version still needs manual instrumentation 

internal static class EventPublisherActivitySource
{
    public const string ActivitySourceName = nameof(EventPublisher);
    
    private const string Name = "event publish";
    private const ActivityKind Kind = ActivityKind.Producer;
    private const string EventTopicTag = "event.topic";
    private const string EventIdTag = "event.id";
    private const string EventTypeTag = "event.type";

    private static readonly ActivitySource ActivitySource
        = new(ActivitySourceName);

    private static readonly TextMapPropagator Propagator
        = Propagators.DefaultTextMapPropagator;

    public static Activity? StartActivity(string topic, Event @event)
    {
        if (!ActivitySource.HasListeners())
        {
            return null;
        }

        // ReSharper disable once ExplicitCallerInfoArgument
        return ActivitySource.StartActivity(
            name: Name,
            kind: Kind,
            tags: new KeyValuePair<string, object?>[]
            {
                new(EventTopicTag, topic),
                new(EventIdTag, @event.Id),
                new(EventTypeTag, @event.GetType().Name),
            });
    }

    public static Dictionary<string, object> EnrichHeadersWithTracingContext(
        Activity? activity,
        Dictionary<string, object> headers)
    {
        if (activity is null)
        {
            return headers;
        }

        // on the receiving side,
        // the service will extract this information
        // to maintain the overall tracing context

        var contextToInject = activity?.Context
                              ?? Activity.Current?.Context
                              ?? default;

        Propagator.Inject(
            new PropagationContext(contextToInject, Baggage.Current),
            headers,
            InjectTraceContext);

        return headers;

        static void InjectTraceContext(Dictionary<string, object> headers, string key, string value)
            => headers.Add(key, value);
    }
}