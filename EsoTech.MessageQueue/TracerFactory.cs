using Microsoft.AspNetCore.Http;
using OpenTracing;
using OpenTracing.Noop;
using Microsoft.Extensions.DependencyInjection;

namespace EsoTech.MessageQueue
{
    public class TracerFactory
    {
        private readonly IHttpContextAccessor? _httpContextAccessor;
        private readonly ITracer? _tracer;

        public ITracer Tracer =>
            _httpContextAccessor?.HttpContext?.RequestServices?.GetService<ITracer>() ??
            _tracer ??
            NoopTracerFactory.Create();

        public TracerFactory(IHttpContextAccessor? httpContextAccessor, ITracer? tracer)
        {
            _httpContextAccessor = httpContextAccessor;
            _tracer = tracer;
        }

        public TracerFactory(IHttpContextAccessor httpContextAccessor) : this(httpContextAccessor, null)
        {
        }

        public TracerFactory(ITracer tracer) : this(null, tracer)
        {
        }

        public TracerFactory() : this(null, null)
        {
        }
    }
}
