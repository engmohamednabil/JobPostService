using Npgsql;
using RabbitMQ.Client;
using System.Text;

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
                string title = reader.GetString(1) ?? string.Empty;
                string postDesc = reader.GetString(2) ?? string.Empty;

                if (!string.IsNullOrEmpty(postDesc))
                {
                    _logger.LogInformation("Create job for item ID {id}: '{postDesc} ", id, postDesc);
                    await passToMQAsync(postDesc);
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
                    _logger.LogInformation("No needed for job id {id}: '{title}'", id, title);
                }
            }
        }

        private async Task passToMQAsync(string postDesc)
        {
            var factory = new ConnectionFactory
            {
                HostName = "localhost", // Hostname or IP of the Docker host
                Port = 5672,           // Port mapped on the Docker host
                UserName = "guest",    // Default RabbitMQ username
                Password = "guest"     // Default RabbitMQ password
            };

            // Establish connection and channel
            using var connection = await factory.CreateConnectionAsync();
            using var channel = await connection.CreateChannelAsync();

            // Declare a queue
            string queueName = "incoming-jobs";
            await channel.QueueDeclareAsync(queue: queueName,
                                 durable: true,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            var body = Encoding.UTF8.GetBytes(postDesc);

            await channel.BasicPublishAsync(exchange: string.Empty, 
                routingKey: queueName, body: body);
        }
    }
}