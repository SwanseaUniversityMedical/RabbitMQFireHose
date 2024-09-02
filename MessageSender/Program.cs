using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.VisualBasic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RabbitMQ.Client;

class MessageSender
{
    static void Main(string[] args)
    {
        string rabbitHost;
        if (args.Length > 0)
            rabbitHost = args[0];
        else
            rabbitHost = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";

        string directoryPath;
        if (args.Length > 1)
            directoryPath = args[1];
        else
            directoryPath = Environment.GetEnvironmentVariable("RABBITMQ_DIR") ??
                            Path.Combine(Environment.CurrentDirectory, "firehose");

        if (!Directory.Exists(directoryPath))
        {
            Directory.CreateDirectory(directoryPath);
        }

        Console.WriteLine("----------------------------------------------------------");
        Console.WriteLine(" FIREHOSE application --> SENDER");
        Console.WriteLine("----------------------------------------------------------");
        Console.WriteLine(" ");
        Console.WriteLine("Input: " + directoryPath);
        Console.WriteLine("RabbitMQ Host: " + rabbitHost);
        Console.WriteLine(" ");
        Console.WriteLine("----------------------------------------------------------");

        // Get all JSON files in the directory, sorted by creation time
        var files = Directory.GetFiles(directoryPath, "*.json")
                             .OrderBy(f => File.GetCreationTime(f))
                             .ToList();

        var factory = new ConnectionFactory() { HostName = rabbitHost };

        using (var connection = factory.CreateConnection())
        using (var channel = connection.CreateModel())
        {
            foreach (var file in files)
            {
                try
                {
                    // Read and deserialize the JSON file
                    string jsonContent = File.ReadAllText(file);
                    var messageData = JsonConvert.DeserializeObject<MessageData>(jsonContent);

                    // Prepare message properties
                    var properties = channel.CreateBasicProperties();
                    properties.Headers = ConvertHeaders(messageData.MessageHeaders);

                    var body = Encoding.UTF8.GetBytes(messageData.MessageBody);

                    // Determine where to send the message
                    if (properties.Headers.ContainsKey("exchange_name") && properties.Headers["exchange_name"].ToString() != "") 
                    {
                        string exchange = properties.Headers["exchange_name"].ToString();
                        foreach (var r in (IList)properties.Headers["routing_keys"])
                        {
                            string routingKey = r.ToString() ?? "";
                            channel.BasicPublish(exchange: exchange, routingKey: routingKey, basicProperties: properties, body: body);
                            Console.WriteLine( $"[x] Sent message to exchange '{exchange}' with routing key '{routingKey}' from file '{file}'");
                        }
                    }
                    else
                    {
                        foreach (var r in (IList)properties.Headers["routing_keys"])
                        {
                            var queueName = r.ToString() ?? "default_queue";
                            channel.QueueDeclare(queue: queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
                            channel.BasicPublish(exchange: "", routingKey: queueName, basicProperties: properties, body: body);
                            Console.WriteLine($"[x] Sent message to queue '{queueName}' from file '{file}'");
                        }
                     
                    }

                    // Optionally delete the file after sending
                    // File.Delete(file);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[!] Failed to process file '{file}': {ex.Message}");
                }
            }
        }

        Console.WriteLine("Message sending complete.");
    }

    // Converts JToken headers to simple types that can be used by RabbitMQ
    static Dictionary<string, object> ConvertHeaders(Dictionary<string, object> headers)
    {
        var convertedHeaders = new Dictionary<string, object>();

        foreach (var header in headers)
        {
            convertedHeaders[header.Key] = ConvertJToken(header.Value);
        }

        return convertedHeaders;
    }

    static object ConvertJToken(object token)
    {
        if (token is JValue jValue)
        {
            return jValue.Value; // Convert JValue to its underlying value
        }
        else if (token is JArray jArray)
        {
            // Convert each element in the JArray
            var array = new List<object>();
            foreach (var item in jArray)
            {
                array.Add(ConvertJToken(item));
            }
            return array;
        }
        else if (token is JObject jObject)
        {
            // Convert each property in the JObject
            var dict = new Dictionary<string, object>();
            foreach (var property in jObject)
            {
                dict[property.Key] = ConvertJToken(property.Value);
            }
            return dict;
        }
        else
        {
            return token;
        }
    }
}

// Class to deserialize the JSON file content
class MessageData
{
    public string Timestamp { get; set; }
    public string MessageBody { get; set; }
    public Dictionary<string, object> MessageHeaders { get; set; }
}