using System.Net.Http.Headers;
using System.Net.Http;
using System.Text;
using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;
using System.Net.Security;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Net.Http.Json;
using EsoTech.MessageQueue.RabbitMQ.Models;
using System.Collections.Generic;

namespace EsoTech.MessageQueue.RabbitMQ.Services
{
    public class RabbitMqManagement : IDisposable
    {
        private SemaphoreSlim _virtualHostLock = new SemaphoreSlim(1);
        private readonly RabbitMQConnectionConfiguration _connection;
        private readonly string _virtualHost;
        private readonly ILogger _logger;

        private HttpClient? _httpClient;
        private HttpClientHandler? _httpHandler;

        private HttpClient HttpClient
        {
            get
            {
                if (_httpClient == null)
                {
                    _httpHandler = new HttpClientHandler();
                    if (_connection.IgnoreSslErrors)
                        _httpHandler.ServerCertificateCustomValidationCallback = (httpRequestMessage, cert, chain, sslPolicyErrors) =>
                        {
                            if (sslPolicyErrors != SslPolicyErrors.None)
                                _logger.LogWarning($"Error during RabbitMQ certificate validation");

                            return true;
                        };

                    var authValue = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{_connection.User}:{_connection.Password}"));
                    _httpClient = new HttpClient(_httpHandler)
                    {
                        BaseAddress = new Uri($"http{(_connection.UseSsl ? "s" : "")}://{_connection.Host}:{_connection.ManagementPort}"),
                        DefaultRequestHeaders = { Authorization = new AuthenticationHeaderValue("Basic", authValue) }
                    };
                }

                return _httpClient;
            }
        }

        public RabbitMqManagement(IOptions<RabbitMQConfiguration> options, ILogger<RabbitMqManagement> logger)
        {
            _connection = options.Value.Connection;
            _virtualHost = _connection.VirtualHost;
            _logger = logger;
        }

        public async Task CreateVirtualHost(CancellationToken cancellationToken)
        {
            await _virtualHostLock.WaitAsync(cancellationToken);

            try
            {
                var managementPort = _connection.ManagementPort;
                var managementHost = _connection.Host;

                using var createVhostResponse = await HttpClient.PutAsync($"api/vhosts/{Uri.EscapeDataString(_virtualHost)}", null, cancellationToken);
                await ConvertErrors(createVhostResponse);

                using var permResponse = await HttpClient.PutAsJsonAsync($"api/permissions/{Uri.EscapeDataString(_virtualHost)}/{_connection.User}", RabbitMqPermissions.AllAllowed, cancellationToken);
                await ConvertErrors(permResponse);
            }
            finally
            {
                _virtualHostLock.Release();
            }
        }

        public async Task<IReadOnlyCollection<RabbitMQQueue>> GetQueueStats(string regexp, CancellationToken cancellationToken)
        {
            return await GetAllItems<RabbitMQQueue>($"api/queues/{Uri.EscapeDataString(_virtualHost)}?page={{0}}&page_size=100&name={regexp}&use_regex=true&pagination=true", cancellationToken);
        }

        private async Task<IReadOnlyCollection<TItem>> GetAllItems<TItem>(string url, CancellationToken cancellationToken)
        {
            using var response = await HttpClient.GetAsync(string.Format(url, 1), cancellationToken);
            await ConvertErrors(response);

            var result = await response.Content.ReadFromJsonAsync<RabbitMQPagedResult<TItem>>(cancellationToken);
            if (result!.PageCount == 1)
                return result.Items!;

            var items = new List<TItem>(result.ItemCount);
            items.AddRange(result.Items ?? []);
            for (int pageNumber = 2; pageNumber <= result.PageCount; pageNumber++)
            {
                using var pageResponse = await HttpClient.GetAsync(string.Format(url, pageNumber), cancellationToken);
                await ConvertErrors(pageResponse);

                result = await pageResponse.Content.ReadFromJsonAsync<RabbitMQPagedResult<TItem>>(cancellationToken);
                items.AddRange(result!.Items ?? []);
            }

            return items;
        }

        public Task<IReadOnlyCollection<RabbitMQQueue>> GetDeadletterQueueStats(CancellationToken cancellationToken) =>
            GetQueueStats($"{Regex.Escape(NamingConvention.DeadletterQueuePostfix)}$", cancellationToken);

        private async Task ConvertErrors(HttpResponseMessage response)
        {
            if (response.IsSuccessStatusCode)
                return;

            var errorContent = await response.Content.ReadAsStringAsync();
            throw new InvalidOperationException($"Request {response.RequestMessage!.Method} {response.RequestMessage.RequestUri} failed\nStatus: {response.StatusCode}\nBody: {errorContent}");
        }

        private class RabbitMqPermissions
        {
            [JsonPropertyName("configure")] public required string Configure { get; init; }
            [JsonPropertyName("write")] public required string Write { get; init; }
            [JsonPropertyName("read")] public required string Read { get; init; }

            public static readonly RabbitMqPermissions AllAllowed = new RabbitMqPermissions { Configure = ".*", Read = ".*", Write = ".*" };
        }

        public void Dispose()
        {
            _virtualHostLock.Dispose();
            _httpClient?.Dispose();
            _httpHandler?.Dispose();
        }
    }
}
