using Confluent.Kafka;
using KafkaExample.Shared;
using Microsoft.AspNetCore.Mvc;
using System.Net;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();


app.MapGet("/", async () =>
{
    const string kafkaServer = "host.docker.internal:29092";
    const string topic = "test";


    var messages = new List<string>();

    for (int i = 1; i <= 10000 ; i++)
    {
        messages.Add(JsonSerializer.Serialize(new MessageRequest{ Id = i, Content = "Content " + i }));
    }

    ProducerConfig config = new ProducerConfig
    {
        BootstrapServers = kafkaServer,
        ClientId = Dns.GetHostName(),
    };

    try
    {
        using var producer = new ProducerBuilder<Null, string>(config).Build();

        foreach (var message in messages)
        {
            await producer.ProduceAsync(topic, new Message<Null, string> { Value = message });
        }

        Task.WhenAll(messages.Select(message => 
            producer.ProduceAsync(topic, new Message<Null, string> { Value = message })
        )).Wait();

        //var message = JsonSerializer.Serialize(new MessageRequest { Id = 0, Content = "Content " });
        //var result = await producer.ProduceAsync(topic, new Message<Null, string> { Value = message});

        return "Sent";
    }
    catch (Exception)
    {
        return "Not sent";
    }
});

app.Run();
