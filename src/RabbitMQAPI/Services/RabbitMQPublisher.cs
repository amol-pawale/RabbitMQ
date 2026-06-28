using System.Text;
using RabbitMQ.Client;

namespace RabbitMQAPI.Services;

public class RabbitMQPublisher : IAsyncDisposable
{
    private IConnection? _connection;
    private IChannel? _channel;
    private readonly string _queueName;
    private readonly string _hostname;

    public RabbitMQPublisher(IConfiguration configuration)
    {
        _hostname = configuration["RabbitMQ:Hostname"] ?? "localhost";
        _queueName = configuration["RabbitMQ:QueueName"] ?? "orders";
    }

    public async Task InitializeAsync(CancellationToken stoppingToken = default)
    {
        var factory = new ConnectionFactory { HostName = _hostname };
        _connection = await factory.CreateConnectionAsync(stoppingToken);
        _channel = await _connection.CreateChannelAsync(cancellationToken: stoppingToken);

        await _channel.QueueDeclareAsync(queue: _queueName,
                                            durable: false,
                                            exclusive: false,
                                            autoDelete: false,
                                            arguments: null,
                                            cancellationToken: stoppingToken);

    }

    public async Task PublishAsync(string message)
    {
        var body = Encoding.UTF8.GetBytes(message);

        await _channel!.BasicPublishAsync(exchange: "",
                                        routingKey: _queueName,
                                        body: body
                                     );
    }

    public async ValueTask DisposeAsync()
    {
        // TODO: close channel and connection
        if (_channel is not null)
            await _channel.DisposeAsync();

        if (_connection is not null)
                await _connection.DisposeAsync();
    }
}
