using Confluent.Kafka;
using KafkaExample.Shared;
using System.Text.Json;

namespace KafkaExample.Consumer
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            const string topic = "test";
            const string groupId = "test_group";
            const string bootstrapServers = "host.docker.internal:29092";
            var config = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumerBuilder = new ConsumerBuilder<Ignore, string>(config).Build();
            consumerBuilder.Subscribe(topic);
            var cancelToken = new CancellationTokenSource();

            try
            {
                while (true)
                {
                    var consumer = consumerBuilder.Consume(cancelToken.Token);
                    var messageRequest = JsonSerializer.Deserialize<MessageRequest>(consumer.Message.Value);

                    Console.WriteLine(consumer.Message.Value);
                    //await Task.Delay(1000, stoppingToken);
                }
            }
            catch (Exception)
            {
                consumerBuilder.Close();
            }
        }
    }
}