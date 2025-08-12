using Npgsql;

namespace JobPostService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly string _connectionString;
        private readonly TimeSpan _interval;

        public Worker(ILogger<Worker> logger, IConfiguration configuration)
        {
            _logger = logger;
            _connectionString = configuration.GetConnectionString("Postgres");
            _interval = TimeSpan.FromSeconds(configuration.GetValue<int>("WorkerSettings:IntervalSeconds"));
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("JobPostService started at: {time}", DateTimeOffset.Now);

            using var timer = new PeriodicTimer(_interval);

            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    _logger.LogInformation("Processing records at: {time}", DateTimeOffset.Now);
                    await ProcessRecordsAsync(stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing records at: {time}", DateTimeOffset.Now);
                }
            }
        }

        private async Task ProcessRecordsAsync(CancellationToken stoppingToken)
        {
            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(stoppingToken);

            var selectCommand = new NpgsqlCommand("SELECT * FROM jobposts", connection);
            using var reader = await selectCommand.ExecuteReaderAsync(stoppingToken);

            while (await reader.ReadAsync(stoppingToken))
            {
                int id = reader.GetInt32(0);
                string title = reader.GetString(1) ?? string.Empty; // Handle NULL values
                string postDesc = title.Trim();

                if (title != postDesc)
                {
                    _logger.LogInformation("Trimming text for ID {id}: '{original}' -> '{trimmed}'", id, title, postDesc);
                    /*
                    // Update the record
                    using var updateConnection = new NpgsqlConnection(_connectionString);
                    await updateConnection.OpenAsync(stoppingToken);
                    var updateCommand = new NpgsqlCommand(
                        "UPDATE records SET text_data = @text, last_updated = CURRENT_TIMESTAMP WHERE id = @id",
                        updateConnection);
                    updateCommand.Parameters.AddWithValue("text", postDesc);
                    updateCommand.Parameters.AddWithValue("id", id);
                    await updateCommand.ExecuteNonQueryAsync(stoppingToken);
                    */
                }
                else
                {
                    _logger.LogInformation("No trimming needed for ID {id}: '{text}'", id, title);
                }
            }
        }
    }
}