using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Persistence.InMem;
using Rebus.Routing.TypeBased;
using Rebus.Sagas;
using Rebus.Sagas.Exclusive;
using Rebus.Transport.InMem;
using System;
using System.Threading.Tasks;

namespace RebusSagaStuckMessages
{
    /// <summary>
    /// Problem using EnforceExclusiveAccess in combination with Saga's
    /// 
    /// This program locks up when two sagas implement use the same sagadata. It does work when not sharing the sagadata
    /// It also fails when registering the same saga twice
    /// </summary>
    class Program
    {
        const string SagaHostQueue = "SagaHostQueue";

        static async Task Main(string[] args)
        {
            using (var activator = new BuiltinHandlerActivator())
            {
                var bus = CreateBus(activator);

                for (int i = 0; i < 10; i++)
                {
                    var id = Guid.NewGuid();
                    Console.WriteLine($"Sending message with id {id}");
                    await bus.Send(new StartSaga { SessionId = id });
                }

                Console.WriteLine("Press 'q' to exit or any other key to repeat");
                Console.ReadKey().ToString();
            }
        }

        static IBus CreateBus(BuiltinHandlerActivator activator)
        {
            activator.Register(() => new SimpleSaga1()); 
            activator.Register(() => new SimpleSaga2()); 

            var network = new InMemNetwork();

            return Configure.With(activator)
                .Transport(t => t.UseInMemoryTransport(network, SagaHostQueue))
                .Options(o =>
                {
                    o.SetNumberOfWorkers(2);
                })
                .Sagas(s =>
                {
                    s.StoreInMemory();
                    s.EnforceExclusiveAccess();
                })
                .Routing(r => r.TypeBased()
                    .Map<StartSaga>(SagaHostQueue))
                .Timeouts(t => t.StoreInMemory())
                .Start();
        }

        public class StartSaga
        {
            public Guid SessionId { get; set; }
        }

        public class SimpleSagaData : SagaData
        {
            public Guid SessionId { get; set; }
        }

        /// <summary>
        /// First saga
        /// </summary>
        public class SimpleSaga1 : Saga<SimpleSagaData>, IAmInitiatedBy<StartSaga>
        {
            protected override void CorrelateMessages(ICorrelationConfig<SimpleSagaData> config)
            {
                config.Correlate<StartSaga>(m => m.SessionId, s => s.SessionId);
            }

            public Task Handle(StartSaga message)
            {
                Console.WriteLine($"Saga 1 Received message with id {message.SessionId}");

                if (!IsNew) return Task.CompletedTask; 

                MarkAsComplete();

                return Task.CompletedTask;
            }
        }

        /// <summary>
        /// Oddly enough using this sagadata class for SimpleSaga2 will no fail the program
        /// </summary>
        public class SimpleSagaData2 : SagaData
        {
            public Guid SessionId { get; set; }
        }

        /// <summary>
        /// Second saga that handles the same message
        /// </summary>
        public class SimpleSaga2 : Saga<SimpleSagaData>, IAmInitiatedBy<StartSaga>
        {
            protected override void CorrelateMessages(ICorrelationConfig<SimpleSagaData> config)
            {
                config.Correlate<StartSaga>(m => m.SessionId, s => s.SessionId);
            }

            public Task Handle(StartSaga message)
            {
                Console.WriteLine($"Saga 2 Received message with id {message.SessionId}");

                if (!IsNew) return Task.CompletedTask;

                MarkAsComplete();

                return Task.CompletedTask;
            }
        }
    }
}
