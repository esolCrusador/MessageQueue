using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.RabbitMQ;
using EsoTech.MessageQueue.RabbitMQ.Services;
using EsoTech.MessageQueue.Testing;
using EsoTech.MessageQueue.Tests.EventHandlers;
using EsoTech.MessageQueue.Tests.Messages;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace EsoTech.MessageQueue.Tests
{

    public abstract class DeadLetterQueueFacts : IAsyncLifetime
    {
        private const int MaxDeliveryCount = 15;
        private readonly ServiceProvider _serviceProvider;
        private FooEventDelegateHandler DelegateHandler => _serviceProvider.GetRequiredService<FooEventDelegateHandler>();
        private EventDelegateHandler<ComplexMessage> GiantDelegateHandler => _serviceProvider.GetRequiredService<EventDelegateHandler<ComplexMessage>>();
        public DeadLetterQueueFacts(IServiceCollection services)
        {
            services.AddLogging();
            services.AddSingleton(new ConfigurationBuilder().AddInMemoryCollection().Build());
            services.AddSingleton<IConfiguration>(sp => sp.GetRequiredService<IConfigurationRoot>());
            _serviceProvider = services.BuildServiceProvider();
        }

        [Trait("Category", "Slow")]
        [Collection(nameof(RabbitMqCollectionCollection))]
        public class SlowRabbitMQ : DeadLetterQueueFacts
        {
            public SlowRabbitMQ(RabbitMqTestFixture rabbitMqTestFixture) : base(
                new ServiceCollection().AddMessageQueue((config, sp) =>
                {
                    config.HandleRealtime = true;
                    config.AckTimeout = TimeSpan.FromMicroseconds(1);
                })
                .AddRabbitMq((opts, cfg) =>
                {
                    rabbitMqTestFixture.Configure(opts, cfg);
                    opts.MaxDeliveryCount = MaxDeliveryCount;
                })
                .AddEventMessageHandler<FooEventDelegateHandler>()
                .AddEventMessageHandler<EventDelegateHandler<ComplexMessage>>()
            )
            {
            }
        }

        //[Trait("Category", "Integration")]
        //public class SlowQARabbitMQ : DeadLetterQueueFacts
        //{
        //    public SlowQARabbitMQ() : base(
        //        new ServiceCollection().AddMessageQueue((config, sp) =>
        //        {
        //            config.HandleRealtime = true;
        //            config.AckTimeout = TimeSpan.FromMicroseconds(1);
        //        })
        //        .AddRabbitMq((opts, cfg) =>
        //        {
        //            opts.Connection.VirtualHost = $"/Testing-{Guid.NewGuid()}";
        //            opts.Connection.Host = "139.59.207.210";
        //            opts.Connection.Port = 5671;
        //            opts.Connection.UseSsl = true;
        //            opts.Connection.IgnoreSslErrors = true;
        //            opts.Connection.User = "admin";
        //            opts.Connection.Password = "284620c4-47f6-470c-a2b1-89d70991eadd";
                    
        //            opts.MaxDeliveryCount = MaxDeliveryCount;
        //        })
        //        .AddEventMessageHandler<FooEventDelegateHandler>()
        //        .AddEventMessageHandler<EventDelegateHandler<ComplexMessage>>()
        //    )
        //    {
        //    }
        //}

        public async Task InitializeAsync()
        {
            if (_serviceProvider.GetService<FakeMessageQueueInitializer>() == null)
                await _serviceProvider.GetRequiredService<IMessageConsumer>().Initialize(default);
        }

        public async Task DisposeAsync()
        {
            await _serviceProvider.DisposeAsync();
        }

        [Fact]
        public async Task Should_Retry_Only_Max_Redelivery_Count_Times()
        {
            int delivers = 0;
            DelegateHandler.Handler = (ev, cancellation) =>
            {
                delivers++;

                throw new Exception();
            };
            await _serviceProvider.GetRequiredService<IMessageQueue>().SendEvent(new FooMsg());

            await MessageQueueTestContext.Wait(() => delivers == MaxDeliveryCount);

            var management = _serviceProvider.GetRequiredService<RabbitMqManagement>();
            await MessageQueueTestContext.Wait(async () => (await management.GetDeadletterQueueStats(default)).Any(q => q.MessagesCount == 1));
        }

        [Fact]
        public async Task Should_Retry_Only_Max_Redelivery_Count_Times_Giant_Message()
        {
            int delivers = 0;
            GiantDelegateHandler.Handler = (ev, cancellation) =>
            {
                delivers++;

                throw new Exception();
            };
            var message = new ComplexMessage
            {
                UserContext = new UserContext
                {
                    BotId = 80,
                    UserContextId = 3265,
                    TenantId = Guid.NewGuid(),
                    UserId = 9606
                },
                OperationId = Guid.NewGuid(),
                SessionId = Guid.NewGuid(),
                Version = 134,
                Identity = Guid.NewGuid().ToString("n"),
                Input = "46531585",
                SurveyNames = null,
                SurveyIds = [405],
                Variables = new Dictionary<string, string?>(),
                StepConfiguration = new StepConfiguration
                {
                    IntegrationId = 101,
                    IntegrationType = "AmoCrm",
                    IntegrationCredentialsId = 4,
                    ActionId = "CreateLead",
                    InputTemplate = "@{\nvar childrenCount =  int.Parse(model.surveys[\"Опрос про путешествие\"][\"Количество детей\"][0]);\nIEnumerable<int> children = Enumerable.Range(1, childrenCount);\nvar childrenAges = string.Join(\", \", children .Select(cn => model.surveys[\"Опрос про путешествие\"][$\"Ребёнок{cn}\"]));\n}\n{\n  \"StatusId\": 73129226,\n  \"PipelineId\": 9087482,\n  \"ContactId\": @model.jsonInput,\n  \"ExtraFields\": {\n    \"Ориентировочные даты отдыха\": \"@model.surveys[\"Опрос про путешествие\"][\"Дата выезда от\"].ToString(\"dd.MM.yy\") - @model.surveys[\"Опрос про путешествие\"][\"Дата выезда до\"].ToString(\"dd.MM.yy\")\",\n    \"Направление\": \"@string.Join(';', model.surveys[\"Опрос про путешествие\"][\"Направление\"])\",\n    \"Количество ночей отдыха\": \"@model.surveys[\"Опрос про путешествие\"][\"Количество ночей\"][0]\",\n    \"Количество туристов (без детей)\": \"@model.surveys[\"Опрос про путешествие\"][\"Количество взрослых\"][0]\",\n    \"Количество детей\": \"@childrenCount\",\n    \"Возраст детей\": \"@childrenAges\",\n    \"Примерный бюджет\": \"@model.surveys[\"Опрос про путешествие\"][\"Примерный бюджет\"]\",\n    \"Тип питания\": \"@string.Join(';', model.surveys[\"Опрос про путешествие\"][\"Тип питания\"])\",\n    \"Уровень отеля\": \"@string.Join(';', model.surveys[\"Опрос про путешествие\"][\"Уровень отеля\"])\",\n    \"В каких странах и отелях отдыхали ранее\": @toJson(model.surveys[\"Опрос про путешествие\"][\"В каких отелях отдыхали ранее?\"])\n  },\n  \"BackupFields\": {\n    \"Направление\": { \"FieldName\": \"Другое направление\", \"PrimaryFieldValue\": \"Другое\" },\n    \"Количество ночей отдыха\": { \"PrimaryFieldValue\": \"Другое\" }\n  }\n}",
                    OutputTemplate = null,
                    Delay = null,
                    Notify = false,
                    Position = new Position
                    {
                        IsEmpty = false,
                        X = 0,
                        Y = 6403.5
                    }
                }

            };
            await _serviceProvider.GetRequiredService<IMessageQueue>().SendEvent(message);

            await MessageQueueTestContext.Wait(() => delivers == MaxDeliveryCount);

            var management = _serviceProvider.GetRequiredService<RabbitMqManagement>();
            await MessageQueueTestContext.Wait(async () => (await management.GetDeadletterQueueStats(default)).Any(q => q.MessagesCount == 1));
        }

        [Fact]
        public async Task Should_Readd_Message_To_Queue()
        {
            int delivers = 0;
            DelegateHandler.Handler = (ev, cancellation) =>
            {
                delivers++;
                throw new Exception();
            };
            await _serviceProvider.GetRequiredService<IMessageQueue>().SendEvent(new FooMsg());

            await MessageQueueTestContext.Wait(() => delivers == MaxDeliveryCount);

            var mq = _serviceProvider.GetRequiredService<RabbitMqMessageQueue>();
            await MessageQueueTestContext.Wait(async () => (await mq.RepublishErrorQueues(null, default)) > 0);

            await MessageQueueTestContext.Wait(() => delivers == MaxDeliveryCount * 2);
        }

        [Fact]
        public async Task Should_Readd_Message_To_Queue_Giant_Message()
        {
            int delivers = 0;
            GiantDelegateHandler.Handler = (ev, cancellation) =>
            {
                delivers++;

                throw new Exception();
            };
            var message = new ComplexMessage
            {
                UserContext = new UserContext
                {
                    BotId = 80,
                    UserContextId = 3265,
                    TenantId = Guid.NewGuid(),
                    UserId = 9606
                },
                OperationId = Guid.NewGuid(),
                SessionId = Guid.NewGuid(),
                Version = 134,
                Identity = Guid.NewGuid().ToString("n"),
                Input = "46531585",
                SurveyNames = null,
                SurveyIds = [405],
                Variables = new Dictionary<string, string?>(),
                StepConfiguration = new StepConfiguration
                {
                    IntegrationId = 101,
                    IntegrationType = "AmoCrm",
                    IntegrationCredentialsId = 4,
                    ActionId = "CreateLead",
                    InputTemplate = "@{\nvar childrenCount =  int.Parse(model.surveys[\"Опрос про путешествие\"][\"Количество детей\"][0]);\nIEnumerable<int> children = Enumerable.Range(1, childrenCount);\nvar childrenAges = string.Join(\", \", children .Select(cn => model.surveys[\"Опрос про путешествие\"][$\"Ребёнок{cn}\"]));\n}\n{\n  \"StatusId\": 73129226,\n  \"PipelineId\": 9087482,\n  \"ContactId\": @model.jsonInput,\n  \"ExtraFields\": {\n    \"Ориентировочные даты отдыха\": \"@model.surveys[\"Опрос про путешествие\"][\"Дата выезда от\"].ToString(\"dd.MM.yy\") - @model.surveys[\"Опрос про путешествие\"][\"Дата выезда до\"].ToString(\"dd.MM.yy\")\",\n    \"Направление\": \"@string.Join(';', model.surveys[\"Опрос про путешествие\"][\"Направление\"])\",\n    \"Количество ночей отдыха\": \"@model.surveys[\"Опрос про путешествие\"][\"Количество ночей\"][0]\",\n    \"Количество туристов (без детей)\": \"@model.surveys[\"Опрос про путешествие\"][\"Количество взрослых\"][0]\",\n    \"Количество детей\": \"@childrenCount\",\n    \"Возраст детей\": \"@childrenAges\",\n    \"Примерный бюджет\": \"@model.surveys[\"Опрос про путешествие\"][\"Примерный бюджет\"]\",\n    \"Тип питания\": \"@string.Join(';', model.surveys[\"Опрос про путешествие\"][\"Тип питания\"])\",\n    \"Уровень отеля\": \"@string.Join(';', model.surveys[\"Опрос про путешествие\"][\"Уровень отеля\"])\",\n    \"В каких странах и отелях отдыхали ранее\": @toJson(model.surveys[\"Опрос про путешествие\"][\"В каких отелях отдыхали ранее?\"])\n  },\n  \"BackupFields\": {\n    \"Направление\": { \"FieldName\": \"Другое направление\", \"PrimaryFieldValue\": \"Другое\" },\n    \"Количество ночей отдыха\": { \"PrimaryFieldValue\": \"Другое\" }\n  }\n}",
                    OutputTemplate = null,
                    Delay = null,
                    Notify = false,
                    Position = new Position
                    {
                        IsEmpty = false,
                        X = 0,
                        Y = 6403.5
                    }
                }

            };
            await _serviceProvider.GetRequiredService<IMessageQueue>().SendEvent(message);

            await MessageQueueTestContext.Wait(() => delivers == MaxDeliveryCount);

            var mq = _serviceProvider.GetRequiredService<RabbitMqMessageQueue>();
            await MessageQueueTestContext.Wait(async () => (await mq.RepublishErrorQueues(null, default)) > 0);

            await MessageQueueTestContext.Wait(() => delivers == MaxDeliveryCount * 2);
        }


        [Fact]
        public async Task Should_Celanup_Message_Queue()
        {
            int delivers = 0;
            DelegateHandler.Handler = (ev, cancellation) =>
            {
                delivers++;
                throw new Exception();
            };
            await _serviceProvider.GetRequiredService<IMessageQueue>().SendEvent(new FooMsg());

            await MessageQueueTestContext.Wait(() => delivers == MaxDeliveryCount);

            var mq = _serviceProvider.GetRequiredService<RabbitMqMessageQueue>();
            await MessageQueueTestContext.Wait(async () => (await mq.CleanupErrorQueues(null, default)) > 0);

            var management = _serviceProvider.GetRequiredService<RabbitMqManagement>();
            await MessageQueueTestContext.Wait(async () => (await management.GetDeadletterQueueStats(default)).Any(q => q.MessagesCount == 0));
        }

        [Fact]
        public async Task Should_Handle_Message_From_DeadLetter()
        {
            int delivers = 0;
            DelegateHandler.Handler = (ev, cancellation) =>
            {
                delivers++;
                throw new Exception();
            };
            await _serviceProvider.GetRequiredService<IMessageQueue>().SendEvent(new FooMsg());

            await MessageQueueTestContext.Wait(() => delivers == MaxDeliveryCount);

            DelegateHandler.Handler = (ev, cancellation) =>
            {
                delivers++;
                return Task.CompletedTask;
            };

            var mq = _serviceProvider.GetRequiredService<RabbitMqMessageQueue>();
            await MessageQueueTestContext.Wait(async () => (await mq.RepublishErrorQueues(null, default)) > 0);

            await MessageQueueTestContext.Wait(() => delivers == MaxDeliveryCount + 1);
        }
    }
}
