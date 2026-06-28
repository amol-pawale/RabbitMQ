using RabbitMQWorker;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<RabbitWorker>();

var host = builder.Build();
host.Run();