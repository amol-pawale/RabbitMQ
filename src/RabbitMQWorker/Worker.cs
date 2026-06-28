using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Threading.Channels;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQWorker;

public class RabbitWorker : BackgroundService, IAsyncDisposable
{
    private readonly ILogger<RabbitWorker> _logger;

    private readonly IConfiguration _configuration;

    private IConnection? _connection;

    private IChannel? _channel;

    public RabbitWorker(ILogger<RabbitWorker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var hostName = _configuration["RabbitMQ:Host"] ?? "localhost";
        var queueName = _configuration["RabbitMQ:QueueName"] ?? "orders";


        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConnectAndConsumeAsync(hostName, queueName, stoppingToken);
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Faild to connect to RabbitMQ Retry in the 5s...");
                await Task.Delay(5000, stoppingToken);
            }
        }

        try
        {

        }
        catch (OperationCanceledException)
        {
            //expected on the shutdown
        }
    }

    private async Task ConnectAndConsumeAsync(string hostname, string queueName, CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = hostname,
            Port = _configuration.GetValue<int>("RabbitMQ:Port", 5672),
            UserName = _configuration["RabbitMQ:Username"] ?? "guest",
            Password = _configuration["RabbitMQ:Password"] ?? "guest"
        };
        _connection = await factory.CreateConnectionAsync(stoppingToken);
        _channel = await _connection.CreateChannelAsync(cancellationToken: stoppingToken);

        _connection.ConnectionShutdownAsync += (sender, args) =>
        {
            _logger.LogWarning("RabiitMQ connection shut down: {Reason}", args.ReplyText);
            return Task.CompletedTask;
        };

        await _channel.QueueDeclareAsync(queue: queueName,
                                            durable: false,
                                            exclusive: false,
                                            autoDelete: false,
                                            arguments: null,
                                            cancellationToken: stoppingToken);

        _logger.LogInformation("Successfully connected to the RabbitMQ. Waiting for message on '{Queue}'....", queueName);

        var consumer = new AsyncEventingBasicConsumer(_channel);

        consumer.ReceivedAsync += async (model, ea) =>
        {
            var body = ea.Body.ToArray();

            try
            {
                var message = Encoding.UTF8.GetString(body);
                _logger.LogInformation(" [x] Received message {Message}", message);

                await _channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message, nacking without the requeue");
                await _channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);

            }
        };

        await _channel.BasicConsumeAsync(queue: queueName,
                                            autoAck: false,
                                            consumer: consumer,
                                            cancellationToken: stoppingToken);

    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (_channel is not null)
                await _channel.CloseAsync(cancellationToken);

            if (_connection is not null)
                await _connection.CloseAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error closing RabbitMQ connection/channel during shutdown");

        }
        await base.StopAsync(cancellationToken);

    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
            await _channel.DisposeAsync();

        if (_connection is not null)
            await _connection.DisposeAsync();
    }
}
