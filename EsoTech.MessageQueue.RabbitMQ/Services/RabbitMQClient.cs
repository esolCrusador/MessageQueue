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
        private readonly RabbitMQConnectionConfiguration _connectionConfiguration;
        private readonly string _virtualHost;
        private readonly ConnectionFactory _factory;
        private readonly RabbitMqManagement _rabbitMqManager;
        private readonly ChannelsPool _channelsPool;
        private readonly ILogger<RabbitMQClient> _logger;

        private IConnection? _connection;
        private readonly object _connectionLock = new object();
        private Task<IConnection> _connectionTask;

        public RabbitMQClient(RabbitMqManagement rabbitMqManager, IOptions<RabbitMQConfiguration> options, ILogger<RabbitMQClient> logger)
        {
            _connectionConfiguration = options.Value.Connection;
            _virtualHost = _connectionConfiguration.VirtualHost;
            _rabbitMqManager = rabbitMqManager;
            _channelsPool = new ChannelsPool(options.Value.SendersPool);
            _logger = logger;

            _factory = new ConnectionFactory
            {
                HostName = _connectionConfiguration.Host ?? throw new ArgumentException($"{nameof(options.Value.Connection)}.{nameof(_connectionConfiguration.Host)} is not configured"),
                Port = _connectionConfiguration.Port,
                UserName = _connectionConfiguration.User ?? throw new ArgumentException($"{nameof(options.Value.Connection)}.{nameof(_connectionConfiguration.User)} is not configured"),
                Password = _connectionConfiguration.Password ?? throw new ArgumentException($"{nameof(options.Value.Connection)}.{nameof(_connectionConfiguration.Password)} is not configured"),
                VirtualHost = _connectionConfiguration.VirtualHost,
            };
            if (_connectionConfiguration.UseSsl)
            {
                _factory.Ssl = new SslOption
                {
                    ServerName = _connectionConfiguration.Host,
                    Enabled = true,
                };
                if (_connectionConfiguration.IgnoreSslErrors)
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

        private async Task<IConnection> GetConnection()
        {
            var connection = _connection;
            if (connection?.IsOpen == true)
                return connection;

            var connectionTask = _connectionTask;
            connection = await connectionTask;

            if (!connection.IsOpen)
            {
                if (connectionTask == _connectionTask)
                    lock (_connectionLock)
                        if (connectionTask == _connectionTask)
                            _connectionTask = _factory.CreateConnectionAsync();

                connection = await _connectionTask;
            }

            return _connection = connection;
        }

        public async Task<IChannel> CreateChannel()
        {
            try
            {
                return await (await GetConnection()).CreateChannelAsync();
            }
            catch (OperationInterruptedException ex) when (ex.Message.Contains($"NOT_ALLOWED - vhost {_virtualHost} not found"))
            {
                await _rabbitMqManager.CreateVirtualHost(default);
                return await CreateChannel();
            }
        }

        public Task<ChannelsPool.ChannelLock> CaptureSenderChannel() => _channelsPool.CaptureChannel(CreateChannel, default);

        public async ValueTask DisposeAsync()
        {
            await _channelsPool.DisposeAsync();
            IConnection? connection = null;
            try
            {
                connection = await GetConnection();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during getting connection from disposal");
            }
            await (connection?.DisposeAsync() ?? default);
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

        public async Task RemoveEventSubscription(IChannel channel, string topicName, string subscriptionName, string rotingKey, CancellationToken cancellationToken)
        {
            await channel.ExchangeUnbindAsync(subscriptionName, topicName, rotingKey, cancellationToken: cancellationToken);
        }

        public Task CreateQueue(IChannel channel, string queueName, CancellationToken cancellationToken) =>
            channel.ExchangeDeclareAsync(queueName, "direct", true, false, cancellationToken: cancellationToken);

        public async Task CreateCommandSubscription(IChannel channel, string queueName, IEnumerable<string> rotingKeys, CancellationToken cancellationToken)
        {
            await CreateQueue(channel, queueName, cancellationToken);

            await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false, autoDelete: false, cancellationToken: cancellationToken);
            await Task.WhenAll(rotingKeys.Select(rk => BindRotingKey(channel, queueName, rk, cancellationToken)));
        }

        public Task RemoveCommandSubscription(IChannel channel, string subscriptionName, string routingKey, CancellationToken cancellationToken)
        {
            return channel.QueueUnbindAsync(subscriptionName, subscriptionName, routingKey, cancellationToken: cancellationToken);
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
