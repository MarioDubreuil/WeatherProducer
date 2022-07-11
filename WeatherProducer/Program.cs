using Confluent.Kafka;
using Newtonsoft.Json;

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092"
};

using var producer = new ProducerBuilder<Null, string>(config).Build();

try
{
    string? state;
    while ((state = Console.ReadLine()) != null)
    {
        var response = await producer.ProduceAsync(
            "weather-topic",
            new Message<Null, string>
            {
                Value =
                    Char.IsLower(state[0])
                        ? JsonConvert.SerializeObject(new Weather(state, 70))
                        : JsonConvert.SerializeObject(new Address("US", state, "Dallas"))
            });
        Console.WriteLine(response.Value);
    }
}
catch (ProduceException<Null, string> exc)
{
    Console.WriteLine(exc.Message);
}

public record Weather(string State, int Temperature);

public record Address(string Country, string State, string City);
