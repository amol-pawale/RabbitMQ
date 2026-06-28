using RabbitMQAPI.Services;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();
builder.Services.AddSingleton<RabbitMQPublisher>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
    app.MapScalarApiReference();
}

app.UseHttpsRedirection();

var publisher = app.Services.GetRequiredService<RabbitMQPublisher>();
await publisher.InitializeAsync();

app.MapPost("/orders", async (string message) =>
{
    await publisher.PublishAsync(message);
    return Results.Ok();
});

app.Run();
