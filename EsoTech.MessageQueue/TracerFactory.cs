using OpenTracing;
using OpenTracing.Noop;

namespace EsoTech.MessageQueue
{
    public class TracerFactory
    {
        private readonly ITracer? _tracer;

        public ITracer Tracer => _tracer ?? NoopTracerFactory.Create();

        public TracerFactory(ITracer? tracer)
        {
            _tracer = tracer;
        }

        public TracerFactory() : this(null)
        {
        }
    }
}
