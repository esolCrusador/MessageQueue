using System;
using System.Collections.Generic;

namespace EsoTech.MessageQueue.Tests.Messages
{
    public class ComplexMessage
    {
        public UserContext UserContext { get; set; }
        public Guid OperationId { get; set; }
        public Guid SessionId { get; set; }
        public int Version { get; set; }
        public string Identity { get; set; }
        public string Input { get; set; }
        public List<string>? SurveyNames { get; set; }
        public List<int> SurveyIds { get; set; }
        public Dictionary<string, string?> Variables { get; set; }
        public StepConfiguration StepConfiguration { get; set; }
    }

    public class Position
    {
        public bool IsEmpty { get; set; }
        public int X { get; set; }
        public double Y { get; set; }
    }

    public class StepConfiguration
    {
        public int IntegrationId { get; set; }
        public string IntegrationType { get; set; }
        public int IntegrationCredentialsId { get; set; }
        public string ActionId { get; set; }
        public string InputTemplate { get; set; }
        public string? OutputTemplate { get; set; }
        public string? Delay { get; set; }
        public bool Notify { get; set; }
        public Position Position { get; set; }
    }

    public class UserContext
    {
        public int BotId { get; set; }
        public int UserContextId { get; set; }
        public Guid TenantId { get; set; }
        public int UserId { get; set; }
    }
}
