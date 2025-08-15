using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.RabbitMQ.Services
{
    public class ChannelsPool : IAsyncDisposable
    {
        private readonly int _senderPool;
        private readonly ConcurrentBag<ChannelLock> _channels;
        private readonly SemaphoreSlim _channelsLock;
        private bool _disposed;
        private int _createdChannels;

        public ChannelsPool(int senderPool)
        {
            _channels = [];
            _channelsLock = new(senderPool);
            _senderPool = senderPool;
        }

        public async ValueTask DisposeAsync()
        {
            _disposed = true;
            _channelsLock.Dispose();

            foreach (var channel in _channels)
                await channel.DisposeAsync();
        }

        public async Task<ChannelLock> CaptureChannel(Func<Task<IChannel>> createChannel, CancellationToken cancellationToken)
        {
            if (_channels.TryTake(out var channel))
                return channel;


            while (true)
            {
                await _channelsLock.WaitAsync(cancellationToken);
                if (_channels.TryTake(out channel))
                    return channel;

                if (_createdChannels < _senderPool)
                {
                    if (Interlocked.Increment(ref _createdChannels) > _senderPool)
                        Interlocked.Decrement(ref _createdChannels);
                    else
                        return new ChannelLock(await createChannel(), this);
                }
            }
        }

        private async ValueTask ReleaseChannel(ChannelLock channelLock)
        {
            if (_disposed)
            {
                await channelLock.Channel.DisposeAsync();
                return;
            }

            if (channelLock.Channel.IsClosed)
            {
                Interlocked.Decrement(ref _createdChannels);
                await channelLock.DisposeAsync();
            }
            else
            {
                _channels.Add(channelLock);
            }

            _channelsLock.Release();
        }

        public class ChannelLock : IAsyncDisposable
        {
            private readonly ChannelsPool _channelsPool;
            public IChannel Channel { get; }

            public ChannelLock(IChannel channel, ChannelsPool channelsPool)
            {
                Channel = channel;
                _channelsPool = channelsPool;
            }
            public ValueTask DisposeAsync() => _channelsPool.ReleaseChannel(this);
        }
    }
}
