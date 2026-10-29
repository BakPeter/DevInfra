using System.Text.Json;
using System.Text.RegularExpressions;
using Confluent.Kafka;
using Npgsql;
using NpgsqlTypes;

namespace service_dotnet
{
    public class ServiceStatePublisher : BackgroundService
    {
        private const string ServiceName = "service-dotnet";
        private const string StateKey = "ServiceState";

        private static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(3);
        private static readonly TimeSpan SinkTimeout = TimeSpan.FromSeconds(5);

        private readonly ILogger<ServiceStatePublisher> _logger;
        private readonly string _connectionString;
        private readonly string _schema;
        private readonly string _serviceStateTable;
        private readonly string _stateTopic;
        private readonly TimeSpan _interval;
        private readonly IProducer<string, string> _producer;
        private readonly IAdminClient _adminClient;

        private bool _schemaReady;

        public ServiceStatePublisher(
            IConfiguration configuration,
            ILogger<ServiceStatePublisher> logger)
        {
            _logger = logger;

            _schema = configuration["POSTGRES_SCHEMA"] ?? "dotnet";

            // A schema name cannot be a bound parameter, so it is interpolated and must be validated.
            if (!Regex.IsMatch(_schema, "^[a-z_][a-z0-9_]*$"))
            {
                throw new InvalidOperationException($"Invalid POSTGRES_SCHEMA: {_schema}");
            }

            _serviceStateTable = $"{_schema}.service_state";

            _connectionString = new NpgsqlConnectionStringBuilder
            {
                Host = configuration["POSTGRES_HOST"] ?? "localhost",
                Port = int.TryParse(configuration["POSTGRES_PORT"], out var pgPort) ? pgPort : 5432,
                Username = configuration["POSTGRES_USER"] ?? "postgres",
                Password = configuration["POSTGRES_PASSWORD"] ?? "postgres",
                Database = configuration["POSTGRES_DB"] ?? "devinfra",
            }.ConnectionString;

            _stateTopic = configuration["KAFKA_TOPIC_STATE_DOTNET_SERVICE"] ?? "service-dotnet.state";

            var intervalSeconds =
                int.TryParse(configuration["DOTNET_SERVICE_STATE_INTERVAL_SECONDS"], out var seconds)
                    ? seconds
                    : 30;

            _interval = TimeSpan.FromSeconds(intervalSeconds);

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = configuration["KAFKA_BOOTSTRAP_SERVERS"] ?? "localhost:9092",
                ClientId = ServiceName,

                // Default is 5 minutes, which would park a tick on a dead broker.
                MessageTimeoutMs = (int)SinkTimeout.TotalMilliseconds,
            };

            _producer = new ProducerBuilder<string, string>(producerConfig)
                // Route librdkafka's own diagnostics through Serilog instead of raw stderr.
                .SetLogHandler((_, message) => _logger.LogDebug(
                    "Kafka client: {Message}",
                    message.Message))
                .SetErrorHandler((_, error) => _logger.LogDebug(
                    "Kafka client error: {Reason}",
                    error.Reason))
                .Build();
            _adminClient = new DependentAdminClientBuilder(_producer.Handle).Build();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation(
                "Service state publisher started every {IntervalSeconds} s publishing to {Topic}",
                _interval.TotalSeconds,
                _stateTopic);

            using var timer = new PeriodicTimer(_interval);

            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    await PublishServiceStateAsync(stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception, "Service state tick failed");
                }
            }
        }

        // The three sinks are independent on purpose: when one is down the state event
        // must still reach the other two.
        private async Task PublishServiceStateAsync(CancellationToken cancellationToken)
        {
            var checks = await ProbeHealthAsync(cancellationToken);
            var unhealthy = checks.Values.Count(result => result != "Healthy");

            var serviceState = new ServiceState
            {
                PublishedAt = DateTimeOffset.UtcNow,
                OriginService = ServiceName,
                HealthState = unhealthy == 0
                    ? "Healthy"
                    : unhealthy == checks.Count
                        ? "Unhealthy"
                        : "Degraded",
                Checks = checks,
            };

            // The sinks run concurrently and each owns its own error handling, so a dead
            // Kafka can neither fail nor delay the Seq and Postgres writes.
            var kafkaWrite = WriteToKafkaAsync(serviceState, cancellationToken);
            var postgresWrite = WriteToPostgresAsync(serviceState, cancellationToken);

            _logger.LogInformation(
                "Service state {HealthState} for {OriginService}. PublishedAt: {PublishedAt}. Checks: {@Checks}",
                serviceState.HealthState,
                serviceState.OriginService,
                serviceState.PublishedAt,
                serviceState.Checks);

            await Task.WhenAll(kafkaWrite, postgresWrite);
        }

        private async Task WriteToKafkaAsync(
            ServiceState serviceState,
            CancellationToken cancellationToken)
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(SinkTimeout);

            try
            {
                await _producer.ProduceAsync(
                    _stateTopic,
                    new Message<string, string>
                    {
                        Key = StateKey,
                        Value = JsonSerializer.Serialize(serviceState),
                    },
                    timeout.Token);
            }
            catch (Exception exception)
            {
                _logger.LogError(
                    exception,
                    "Failed to publish service state to Kafka. Topic: {Topic}",
                    _stateTopic);
            }
        }

        private async Task WriteToPostgresAsync(
            ServiceState serviceState,
            CancellationToken cancellationToken)
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(SinkTimeout);

            try
            {
                await StoreServiceStateAsync(serviceState, timeout.Token);
            }
            catch (Exception exception)
            {
                _logger.LogError(
                    exception,
                    "Failed to store service state in Postgres. Table: {Table}",
                    _serviceStateTable);
            }
        }

        private async Task<IDictionary<string, string>> ProbeHealthAsync(
            CancellationToken cancellationToken)
        {
            var checks = new Dictionary<string, string>();

            using var probeTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            probeTimeout.CancelAfter(ProbeTimeout);

            try
            {
                await using var connection = new NpgsqlConnection(_connectionString);
                await connection.OpenAsync(probeTimeout.Token);

                await using var command = new NpgsqlCommand("SELECT 1", connection);
                await command.ExecuteScalarAsync(probeTimeout.Token);

                checks["postgres"] = "Healthy";
            }
            catch (Exception exception)
            {
                checks["postgres"] = "Unhealthy";
                _logger.LogError(exception, "Postgres health probe failed");
            }

            try
            {
                // A real metadata round-trip, so a stopped broker is actually detected.
                _adminClient.GetMetadata(ProbeTimeout);

                checks["kafka"] = "Healthy";
            }
            catch (Exception exception)
            {
                checks["kafka"] = "Unhealthy";
                _logger.LogError(exception, "Kafka health probe failed");
            }

            return checks;
        }

        private async Task StoreServiceStateAsync(
            ServiceState serviceState,
            CancellationToken cancellationToken)
        {
            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            if (!_schemaReady)
            {
                await EnsureSchemaAsync(connection, cancellationToken);
            }

            await using var command = new NpgsqlCommand(
                $@"INSERT INTO {_serviceStateTable}
                       (published_at, origin_service, health_state, checks)
                   VALUES (@published_at, @origin_service, @health_state, @checks)",
                connection);

            command.Parameters.AddWithValue("published_at", serviceState.PublishedAt);
            command.Parameters.AddWithValue("origin_service", serviceState.OriginService);
            command.Parameters.AddWithValue("health_state", serviceState.HealthState);
            command.Parameters.Add(new NpgsqlParameter("checks", NpgsqlDbType.Jsonb)
            {
                Value = JsonSerializer.Serialize(serviceState.Checks),
            });

            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        private async Task EnsureSchemaAsync(
            NpgsqlConnection connection,
            CancellationToken cancellationToken)
        {
            await using var command = new NpgsqlCommand(
                $@"CREATE SCHEMA IF NOT EXISTS {_schema};
                   CREATE TABLE IF NOT EXISTS {_serviceStateTable} (
                       id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                       published_at timestamptz NOT NULL,
                       origin_service text NOT NULL,
                       health_state text NOT NULL,
                       checks jsonb NOT NULL
                   );
                   CREATE INDEX IF NOT EXISTS service_state_published_at_idx
                       ON {_serviceStateTable} (published_at DESC);",
                connection);

            await command.ExecuteNonQueryAsync(cancellationToken);

            _schemaReady = true;

            _logger.LogInformation(
                "Postgres schema ready {Schema} in {Database}",
                _schema,
                connection.Database);
        }

        public override void Dispose()
        {
            _producer.Flush(TimeSpan.FromSeconds(5));
            _adminClient.Dispose();
            _producer.Dispose();

            base.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
