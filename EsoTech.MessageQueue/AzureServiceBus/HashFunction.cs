using System;
using System.Collections.Concurrent;
using System.Security.Cryptography;

namespace EsoTech.MessageQueue.AzureServiceBus
{
    public class HashFunction: IDisposable
    {
        private MD5? _md5;

        private readonly ConcurrentDictionary<string, string> _hashes = new ConcurrentDictionary<string, string>();
        public string GetHash(string input) =>
            _hashes.GetOrAdd(input, _ => ComputeMd5Hash(input));

        private string ComputeMd5Hash(string input)
        {
            var md5 = _md5 ??= MD5.Create();

            byte[] inputBytes = System.Text.Encoding.ASCII.GetBytes(input);
            byte[] hashBytes = md5.ComputeHash(inputBytes);

            return Convert.ToBase64String(hashBytes);
        }

        public void Dispose()
        {
            _md5?.Dispose();
        }
    }
}
