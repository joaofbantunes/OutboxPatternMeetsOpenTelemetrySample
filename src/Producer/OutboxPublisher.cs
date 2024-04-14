using System.Data;
using System.Text.Json;
using Dapper;
using Npgsql;

namespace Producer;

internal class OutboxPublisher(
    NpgsqlDataSource dataSource,
    EventPublisher eventPublisher,
    OutboxListener outboxListener) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Delay(TimeSpan.FromSeconds(5));
        const int batchSize = 100;
        while (!stoppingToken.IsCancellationRequested)
        {
            var messagesRead = 0;
            using (var _ = OutboxPublisherActivitySource.StartIterationActivity())
            {
                await using var connection = await dataSource.OpenConnectionAsync(stoppingToken);
                var tx = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, stoppingToken);
                var messages = (await connection.QueryAsync<OutboxMessage>(
                    // lang=postgresql
                    """
                    SELECT id as Id, payload as Payload, telemetry_context as TelemetryContext, created_at as CreatedAt
                    FROM public.outbox
                    ORDER BY id
                    LIMIT @BatchSize;
                    """,
                    new { BatchSize = batchSize })).ToArray();

                if (messages.Length > 0)
                {
                    await connection.ExecuteAsync(
                        // lang=postgresql
                        "DELETE FROM public.outbox WHERE id = any(@Ids);",
                        new { Ids = messages.Select(m => m.Id).ToArray() });
                }

                foreach (var message in messages)
                {
                    using var activity = OutboxPublisherActivitySource.StartActivityFromPersistedContext(
                        message.TelemetryContext);
                    eventPublisher.Publish(JsonSerializer.Deserialize<Event>(message.Payload)!);
                    ++messagesRead;
                }

                await tx.CommitAsync(stoppingToken);
            }

            if (messagesRead < batchSize)
            {
                await Task.WhenAny(
                    outboxListener.WaitForMessagesAsync(stoppingToken),
                    Task.Delay(TimeSpan.FromSeconds(30), stoppingToken));
            }
        }
    }
}

public record OutboxMessage(
    int Id,
    string Payload,
    string? TelemetryContext,
    DateTime CreatedAt);