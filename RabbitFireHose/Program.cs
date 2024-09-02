using System.Text;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Formatting = System.Xml.Formatting;

namespace RabbitFireHose
{
    internal class Program
    {
        private const string queueName = "FireHose";
        private static int fileCounter = 0;

        static void Main(string[] args)
        {
            string rabbitHost;
            if (args.Length > 0)
                rabbitHost = args[0];
            else
                rabbitHost = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "loadingbay";

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
            Console.WriteLine(" FIREHOSE application, streaming RabbitMQ message to disk");
            Console.WriteLine("----------------------------------------------------------");
            Console.WriteLine(" ");
            Console.WriteLine("Need to enable firehose feature: rabbitmqctl trace_on");
            Console.WriteLine("Output: "+ directoryPath);
            Console.WriteLine("Queue used: "+queueName);
            Console.WriteLine("RabbitMQ Host: "+rabbitHost);
            Console.WriteLine(" ");
            Console.WriteLine("----------------------------------------------------------");

            var factory = new ConnectionFactory() { HostName = rabbitHost };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: queueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                // Bind the queue to the exchange "amq.rabbitmq.trace"
                string exchangeName = "amq.rabbitmq.trace";
                string routingKey = "#";  // Bind with a routing key; '#' matches all routing keys
                channel.QueueBind(queue: queueName,
                    exchange: exchangeName,
                    routingKey: routingKey);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    var headers = ea.BasicProperties.Headers;

                    var messageData = new
                    {
                        Timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"),
                        MessageBody = message,
                        MessageHeaders = GetHeadersDictionary(headers)
                    };


                    string json = JsonConvert.SerializeObject(messageData, (Newtonsoft.Json.Formatting)Formatting.Indented);

                    string timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss-fff");
                    string filename = $"message_{timestamp}_{fileCounter:D2}.json";
                    string filepath = Path.Combine(directoryPath, filename);

                    File.WriteAllText(filepath, json);

                    fileCounter = (fileCounter + 1) % 100;

                    Console.WriteLine($"[x] Received and saved message to {filename}");
                };

                channel.BasicConsume(queue: queueName,
                                     autoAck: true,
                                     consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }

        static Dictionary<string, object> GetHeadersDictionary(IDictionary<string, object> headers)
        {
            var headersDict = new Dictionary<string, object>();

            if (headers != null)
            {
                foreach (var header in headers)
                {
                    headersDict[header.Key] = GetHeaderValue(header.Value);
                }
            }

            return headersDict;
        }

        static object GetHeaderValue(object value)
        {
            if (value is byte[] byteArray)
            {
                return Encoding.UTF8.GetString(byteArray);
            }
            else if (value is List<object> list)
            {
                var listValues = new List<object>();
                foreach (var item in list)
                {
                    listValues.Add(GetHeaderValue(item));
                }
                return listValues;
            }
            else
            {
                return value;
            }
        }
    }
}
