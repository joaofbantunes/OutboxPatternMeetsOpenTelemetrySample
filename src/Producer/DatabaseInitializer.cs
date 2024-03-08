using Npgsql;

namespace Producer;

internal class DatabaseInitializer(NpgsqlDataSource dataSource) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await using var command = dataSource.CreateCommand();
        // lang=postgresql
        command.CommandText = """
                                  CREATE TABLE IF NOT EXISTS outbox (
                                      id SERIAL PRIMARY KEY,
                                      payload JSONB NOT NULL,
                                      telemetry_context JSONB,
                                      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                                  );
                              """;
        await command.ExecuteNonQueryAsync(stoppingToken);
    }
}