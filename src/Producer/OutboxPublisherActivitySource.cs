using System.Diagnostics;
using System.Text.Json;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

namespace Producer;

internal static class OutboxPublisherActivitySource
{
    public const string ActivitySourceName = "outbox";

    private static readonly TextMapPropagator Propagator
        = Propagators.DefaultTextMapPropagator;

    private static readonly ActivitySource ActivitySource
        = new(ActivitySourceName);

    public static Activity? StartIterationActivity()
    {
        if (!ActivitySource.HasListeners())
        {
            return null;
        }

        // ReSharper disable once ExplicitCallerInfoArgument
        return ActivitySource.StartActivity(
            name: "outbox iteration",
            kind: ActivityKind.Internal);
    }

    public static Activity? StartActivityFromPersistedContext(string? telemetryContext)
    {
        if (string.IsNullOrWhiteSpace(telemetryContext) || !ActivitySource.HasListeners())
        {
            return null;
        }

        // we're gonna use a custom parent context to look like we're part of the same request flow
        // but, to not lose completely the context of the outbox background job, we're gonna link to it
        var links = Activity.Current is { } currentActivity
            ? new[] { new ActivityLink(currentActivity.Context) }
            : default;

        var deserializedContext = JsonSerializer.Deserialize<List<ContextEntry>>(telemetryContext)!;

        var parentContext = ExtractParentContext(deserializedContext);
        Baggage.Current = parentContext.Baggage;

        return ActivitySource.StartActivity(
            "outbox message publish",
            ActivityKind.Internal,
            parentContext.ActivityContext,
            links: links);

        static PropagationContext ExtractParentContext(
            List<ContextEntry> storedContext)
        {
            var parentContext = Propagator.Extract(
                default,
                storedContext,
                ExtractEntry);

            return parentContext;
        }

        static IEnumerable<string> ExtractEntry(
            List<ContextEntry> context,
            string key)
        {
            foreach (var entry in context)
            {
                if (entry.Key == key)
                {
                    yield return entry.Value;
                }
            }
        }
    }

    public static string? ExtractTelemetryContextForPersistence()
    {
        var activity = Activity.Current;

        if (activity is null)
        {
            return null;
        }

        var extractedContext = new List<ContextEntry>();

        Propagator.Inject(
            new PropagationContext(activity.Context, Baggage.Current),
            extractedContext,
            InjectEntry);

        return JsonSerializer.Serialize(extractedContext);

        static void InjectEntry(
            List<ContextEntry> context,
            string key,
            string value)
            => context.Add(new(key, value));
    }

    private record struct ContextEntry(string Key, string Value);
}