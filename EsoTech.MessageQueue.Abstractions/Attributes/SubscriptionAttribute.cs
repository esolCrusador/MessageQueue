using System;

namespace EsoTech.MessageQueue.Abstractions.Attributes
{
    [AttributeUsage(AttributeTargets.Class)]
    public class SubscriptionAttribute : Attribute
    {
        private string? _prefix;
        private string? _name;
        private string? _postfix;

        public string? Prefix { get => _prefix; set => _prefix = value?.ToLowerInvariant(); }
        public string? Name { get => _name; set => _name = value?.ToLowerInvariant(); }
        public string? Postfix { get => _postfix; set => _postfix = value?.ToLowerInvariant(); }
    }
}
