using Microsoft.Extensions.Options;
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.Net.Security;
using Microsoft.Extensions.Logging;

namespace EsoTech.MessageQueue.RabbitMQ.Services
{
    public class RabbitMQClient : IAsyncDisposable
    {
        private readonly RabbitMQConnectionConfiguration _connection;
        private readonly string _virtualHost;
        private readonly ConnectionFactory _factory;
        private readonly RabbitMqManagement _rabbitMqManager;
        private readonly ILogger<RabbitMQClient> _logger;
        private Task<IConnection> _connectionTask;

        private SemaphoreSlim _senderChannelLock = new SemaphoreSlim(1);
        private IChannel? _senderChannel;

        public async Task<IChannel> CreateChannel()
        {
            try
            {
                return await (await _connectionTask).CreateChannelAsync();
            }
            catch (OperationInterruptedException ex) when (ex.Message.Contains($"NOT_ALLOWED - vhost {_virtualHost} not found"))
            {
                await _rabbitMqManager.CreateVirtualHost(default);
                return await CreateChannel();
            }
        }

        public async Task<IChannel> GetSenderChannel()
        {
            if (_senderChannel != null && _senderChannel.IsOpen)
                return _senderChannel;

            await _senderChannelLock.WaitAsync();

            try
            {
                if (_senderChannel != null && _senderChannel.IsOpen)
                    return _senderChannel;

                return _senderChannel = await CreateChannel();
            }
            finally
            {
                _senderChannelLock.Release();
            }
        }

        public RabbitMQClient(RabbitMqManagement rabbitMqManager, IOptions<RabbitMQConfiguration> options, ILogger<RabbitMQClient> logger)
        {
            _connection = options.Value.Connection;
            _virtualHost = _connection.VirtualHost;
            _rabbitMqManager = rabbitMqManager;
            _logger = logger;

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
                if (_connection.IgnoreSslErrors)
                {
                    _factory.Ssl.AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateChainErrors | SslPolicyErrors.RemoteCertificateNameMismatch;
                    _factory.Ssl.CertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) =>
                    {
                        if (sslPolicyErrors != SslPolicyErrors.None)
                            _logger.LogWarning($"Error during RabbitMQ certificate validation");

                        return true;
                    };
                }
            }

            _connectionTask = _factory.CreateConnectionAsync();
        }

        private static async Task<IChannel> CreateChannel(Task<IConnection> connection) => await (await connection).CreateChannelAsync();

        public async ValueTask DisposeAsync()
        {
            await (await _connectionTask).DisposeAsync();
            await (_senderChannel?.DisposeAsync() ?? default);
            _senderChannelLock.Dispose();
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
            var deadletterQueueName = $"{queueName}{NamingConvention.DeadletterQueuePostfix}";

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
