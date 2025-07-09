using Microsoft.Extensions.Options;
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Headers;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Threading;

namespace EsoTech.MessageQueue.RabbitMQ
{
    public class RabbitMQClient : IAsyncDisposable
    {
        private readonly RabbitMQConnectionConfiguration _connection;
        private readonly string _virtualHost;
        private readonly ConnectionFactory _factory;

        private Task<IConnection> _connectionTask;

        public async Task<IChannel> CreateChannel()
        {
            try
            {
                return await (await _connectionTask).CreateChannelAsync();
            }
            catch (OperationInterruptedException ex) when (ex.Message.Contains($"NOT_ALLOWED - vhost {_virtualHost} not found"))
            {
                await CreateVirtualHost(default);
                return await CreateChannel();
            }
        }

        public RabbitMQClient(IOptions<RabbitMQConfiguration> options)
        {
            _connection = options.Value.Connection;
            _virtualHost = _connection.VirtualHost;
            _factory = new ConnectionFactory
            {
                HostName = _connection.Host ?? throw new ArgumentException($"{nameof(options.Value.Connection)}.{nameof(_connection.Host)} is not configured"),
                Port = _connection.Port,
                UserName = _connection.User ?? throw new ArgumentException($"{nameof(options.Value.Connection)}.{nameof(_connection.User)} is not configured"),
                Password = _connection.Password ?? throw new ArgumentException($"{nameof(options.Value.Connection)}.{nameof(_connection.Password)} is not configured"),
                VirtualHost = _connection.VirtualHost,
            };
            if (_connection.UseSsl)
            {
                _factory.Ssl = new SslOption
                {
                    ServerName = _connection.Host,
                    Enabled = true,
                };
            }

            _connectionTask = _factory.CreateConnectionAsync();
        }

        private SemaphoreSlim _virtualHostLock = new SemaphoreSlim(1);
        public async Task CreateVirtualHost(CancellationToken cancellationToken)
        {
            await _virtualHostLock.WaitAsync(cancellationToken);

            try
            {
                var managementPort = _connection.ManagementPort;
                var managementHost = _connection.Host;

                using var httpClient = new HttpClient();

                var authValue = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{_connection.User}:{_connection.Password}"));
                httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", authValue);

                using var createVhostResponse = await httpClient.PutAsync(
                     $"http://{managementHost}:{managementPort}/api/vhosts/{Uri.EscapeDataString(_virtualHost)}",
                     null,
                     cancellationToken
                );

                if (!createVhostResponse.IsSuccessStatusCode)
                {
                    var errorContent = await createVhostResponse.Content.ReadAsStringAsync();
                    throw new InvalidOperationException($"Failed to create vhost: {createVhostResponse.StatusCode} - {errorContent}");
                }

                // Set permissions
                using var permContent = new StringContent(
                    JsonSerializer.Serialize(new
                    {
                        configure = ".*",
                        write = ".*",
                        read = ".*"
                    }),
                    Encoding.UTF8,
                    "application/json");

                using var permResponse = await httpClient.PutAsync(
                    $"http://{managementHost}:{managementPort}/api/permissions/" +
                    $"{Uri.EscapeDataString(_virtualHost)}/{_connection.User}",
                    permContent, cancellationToken);

                if (!permResponse.IsSuccessStatusCode)
                {
                    var errorContent = await permResponse.Content.ReadAsStringAsync();
                    throw new InvalidOperationException($"Failed to set permissions: {permResponse.StatusCode} - {errorContent}");
                }
            }
            finally
            {
                _virtualHostLock.Release();
            }
        }

        private static async Task<IChannel> CreateChannel(Task<IConnection> connection) => await (await connection).CreateChannelAsync();

        public async ValueTask DisposeAsync()
        {
            await (await _connectionTask).DisposeAsync();
            _virtualHostLock.Dispose();
        }

        public Task CreateTopic(IChannel channel, string topicName, CancellationToken cancellationToken) =>
            channel.ExchangeDeclareAsync(topicName, "topic", true, cancellationToken: cancellationToken);

        public async Task CreateEventSubscription(IChannel channel, string topicName, string subscriptionName, IEnumerable<string> rotingKeys, CancellationToken cancellationToken)
        {
            await CreateTopic(channel, topicName, cancellationToken);

            await channel.ExchangeDeclareAsync(subscriptionName, "direct", true, false, cancellationToken: cancellationToken);
            await Task.WhenAll(rotingKeys.Select(rk => channel.ExchangeBindAsync(subscriptionName, topicName, rk, cancellationToken: cancellationToken)));

            await channel.QueueDeclareAsync(subscriptionName, durable: true, exclusive: false, autoDelete: false, cancellationToken: cancellationToken);

            await Task.WhenAll(rotingKeys.Select(rk => BindRotingKey(channel, subscriptionName, rk, cancellationToken)));
        }

        public Task CreateQueue(IChannel channel, string queueName, CancellationToken cancellationToken) =>
            channel.ExchangeDeclareAsync(queueName, "direct", true, false, cancellationToken: cancellationToken);

        public async Task CreateCommandSubscription(IChannel channel, string queueName, IEnumerable<string> rotingKeys, CancellationToken cancellationToken)
        {
            await CreateQueue(channel, queueName, cancellationToken);

            await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false, autoDelete: false, cancellationToken: cancellationToken);
            await Task.WhenAll(rotingKeys.Select(rk => BindRotingKey(channel, queueName, rk, cancellationToken)));
        }

        public async Task<string> CreateDeadletterQueue(IChannel channel, string queueName, CancellationToken cancellationToken)
        {
            var deadletterQueueName = $"{queueName}-deadletter";

            await CreateQueue(channel, deadletterQueueName, cancellationToken);
            await channel.QueueDeclareAsync(deadletterQueueName, durable: true, exclusive: false, autoDelete: false, cancellationToken: cancellationToken);
            await channel.QueueBindAsync(deadletterQueueName, deadletterQueueName, "", cancellationToken: cancellationToken);

            return deadletterQueueName;
        }

        private Task BindRotingKey(IChannel channel, string subscriptionName, string rotingKey, CancellationToken cancellationToken)
        {
            return channel.QueueBindAsync(subscriptionName, subscriptionName, rotingKey, cancellationToken: cancellationToken);
        }
    }
}
