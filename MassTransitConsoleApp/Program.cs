using System;
using System.Threading;
using MassTransit;
using Microsoft.Extensions.Logging;

namespace MassTransitConsoleApp
{
	class Program
	{
		static void Main(string[] args)
		{
			DemoRootScope_MessageAConsumer_MessageBConsumer();
			Thread.Sleep(250);

			DemoScoped_MessageAConsumer_MessageBConsumer();
			Thread.Sleep(250);

			DemoScoped_MessageAaConsumer1_MessageAaConsumer2();
			Thread.Sleep(250);

			DemoScoped_HigherPriorityMessageAaConsumer1_LowerPriorityMessageAaConsumer2();
		}

		private static void DemoRootScope_MessageAConsumer_MessageBConsumer()
		{
			(string idA, string queueA, int priorityA, ushort prefetchCountA) =
			(
				$"{nameof(DemoRootScope_MessageAConsumer_MessageBConsumer)} Consumer A",
				$"{nameof(DemoRootScope_MessageAConsumer_MessageBConsumer)}-{nameof(IMessageA)}",
				1,
				0
			);

			(string idB, string queueB, int priorityB, ushort prefetchCountB) =
			(
				$"{nameof(DemoRootScope_MessageAConsumer_MessageBConsumer)} Consumer B",
				$"{nameof(DemoRootScope_MessageAConsumer_MessageBConsumer)}-{nameof(IMessageB)}",
				1,
				0
			);

			using (var messageAConsumerBus =
				new MassTransitConsumer<IMessageA, MessageAConsumer>(idA, queueA, priorityA, prefetchCountA))
			using (var messageBConsumerBus =
				new MassTransitConsumer<IMessageB, MessageBConsumer>(idB, queueB, priorityB, prefetchCountB))
			{
				using (var publisherBus = new MassTransitPublisher($"{nameof(DemoScoped_MessageAConsumer_MessageBConsumer)} Publisher"))
				{
					Console.WriteLine("================================================");
					Console.WriteLine("This demo has 2 consumers and 1 publisher");
					Console.WriteLine("Each runs its own bus, created at root or global DI scope");
					Console.WriteLine($"Publisher publishes {nameof(IMessageA)} and {nameof(IMessageB)} message");
					Console.WriteLine($"One consumer takes {nameof(IMessageA)} message");
					Console.WriteLine($"Second consumer takes {nameof(IMessageB)} message");
					Console.WriteLine("================================================");
					Console.WriteLine("Press any key to start");
					Console.ReadKey();

					messageAConsumerBus.BusControl.StartAsync(new CancellationToken());
					messageAConsumerBus.Logger.LogInformation($"[{messageAConsumerBus.Id}] Bus started in scope");

					messageBConsumerBus.BusControl.StartAsync(new CancellationToken());
					messageBConsumerBus.Logger.LogInformation($"[{messageBConsumerBus.Id}] Bus started in scope");

					publisherBus.BusControl.Start();
					publisherBus.Logger.LogInformation($"[{publisherBus.Id}] Bus started in scope");

					var messageA = new MessageA(1);
					publisherBus.BusControl.Publish<IMessageA>(messageA);
					publisherBus.Logger.LogInformation($"[{publisherBus.Id}] Published MessageA {messageA}");

					Thread.Sleep(250);
					var messageB = new MessageB(1);
					publisherBus.BusControl.Publish<IMessageB>(messageB);
					publisherBus.Logger.LogInformation($"[{publisherBus.Id}] Published MessageB {messageB}");

					Console.ReadKey();
				}
			}
		}

		private static void DemoScoped_MessageAConsumer_MessageBConsumer()
		{
			string idA = $"{nameof(DemoScoped_MessageAConsumer_MessageBConsumer)}{nameof(MessageAConsumer)}";
			string idB = $"{nameof(DemoScoped_MessageAConsumer_MessageBConsumer)}{nameof(MessageBConsumer)}";

			string queueA = $"{nameof(DemoScoped_MessageAConsumer_MessageBConsumer)}{nameof(MessageAConsumer)}";
			string queueB = $"{nameof(DemoScoped_MessageAConsumer_MessageBConsumer)}{nameof(MessageBConsumer)}";

			int priorityA = 1;
			int priorityB = 1;

			ushort prefetchCountA = 0;
			ushort prefetchCountB = 0;

			using (var messageAConsumerBus =
				new MassTransitConsumer<IMessageA, MessageAConsumer>(idA, queueA, priorityA, prefetchCountA))
			using (var messageBConsumerBus =
				new MassTransitConsumer<IMessageB, MessageBConsumer>(idB, queueB, priorityB, prefetchCountB))
			{
				using (var publisherBus = new MassTransitPublisher($"{nameof(DemoScoped_MessageAConsumer_MessageBConsumer)} Publisher"))
				{
					Console.WriteLine("Press any clear and continue");
					Console.WriteLine("================================================");
					Console.ReadKey();
					Console.Clear();
					Console.WriteLine("================================================");
					Console.WriteLine("This demo has 2 consumers and 1 publisher");
					Console.WriteLine("Each runs its own bus, in its own DI scope");
					Console.WriteLine($"Publisher publishes {nameof(IMessageA)} and {nameof(IMessageB)} message");
					Console.WriteLine($"One consumer takes {nameof(IMessageA)} message");
					Console.WriteLine($"Second consumer takes {nameof(IMessageB)} message");
					Console.WriteLine("================================================");
					Console.WriteLine("Press any key start");
					Console.ReadKey();

					messageAConsumerBus.ScopedBusControl.StartAsync(new CancellationToken());
					messageAConsumerBus.Logger.LogInformation($"[{messageAConsumerBus.Id}] Bus started in scope");

					messageBConsumerBus.ScopedBusControl.StartAsync(new CancellationToken());
					messageBConsumerBus.Logger.LogInformation($"[{messageBConsumerBus.Id}] Bus started in scope");

					publisherBus.ScopedBusControl.Start();
					publisherBus.Logger.LogInformation($"[{publisherBus.Id}] Bus started in scope");

					var messageA = new MessageA(1);
					publisherBus.ScopedBusControl.Publish<IMessageA>(messageA);
					publisherBus.Logger.LogInformation($"[{publisherBus.Id}] Published MessageA {messageA}");

					Thread.Sleep(250);
					var messageB = new MessageB(1);
					publisherBus.ScopedBusControl.Publish<IMessageB>(messageB);
					publisherBus.Logger.LogInformation($"[{publisherBus.Id}] Published MessageB {messageB}");

					Console.ReadKey();
				}
			}
		}

		private static void DemoScoped_MessageAaConsumer1_MessageAaConsumer2()
		{
			string idAa1 = $"{nameof(DemoScoped_MessageAaConsumer1_MessageAaConsumer2)}{nameof(MessageAaConsumer1)} 1";
			string idAa2 = $"{nameof(DemoScoped_MessageAaConsumer1_MessageAaConsumer2)}{nameof(MessageAaConsumer2)} 2";

			string queueAa = $"{nameof(DemoScoped_MessageAaConsumer1_MessageAaConsumer2)}{nameof(IMessageAa)}";

			int priorityAa1 = 1;
			int priorityAa2 = 1;

			ushort prefetchCountAa1 = 0;
			ushort prefetchCountAa2 = 0;

			using (var messageAConsumerBus1 =
				new MassTransitConsumer<IMessageAa, MessageAaConsumer1>(idAa1, queueAa, priorityAa1, prefetchCountAa1))
			using (var messageAConsumerBus2 =
				new MassTransitConsumer<IMessageAa, MessageAaConsumer2>(idAa2, queueAa, priorityAa2, prefetchCountAa2))
			{
				using (var publisherBus = new MassTransitPublisher($"{nameof(DemoScoped_MessageAaConsumer1_MessageAaConsumer2)}Publisher"))
				{
					Console.WriteLine("Press any clear and continue");
					Console.WriteLine("================================================");
					Console.ReadKey();
					Console.Clear();
					Console.WriteLine("================================================");
					Console.WriteLine("This demo has 2 consumers and 1 publisher");
					Console.WriteLine("Each runs its own bus, in its own DI scope");
					Console.WriteLine($"Publisher publishes 2 {nameof(IMessageAa)} messages");
					Console.WriteLine($"Both consumer takes {nameof(IMessageAa)} message and run with same priority");
					Console.WriteLine("================================================");
					Console.WriteLine("Press any key start");
					Console.ReadKey();

					messageAConsumerBus1.ScopedBusControl.StartAsync(new CancellationToken());
					messageAConsumerBus1.Logger.LogInformation($"[{messageAConsumerBus1.Id}] Bus started in scope");

					messageAConsumerBus2.ScopedBusControl.StartAsync(new CancellationToken());
					messageAConsumerBus2.Logger.LogInformation($"[{messageAConsumerBus2.Id}] Bus started in scope");

					publisherBus.ScopedBusControl.Start();
					publisherBus.Logger.LogInformation($"[{publisherBus.Id}] Bus started in scope");

					var messageAa1 = new MessageAa(1);
					publisherBus.ScopedBusControl.Publish<IMessageAa>(messageAa1);
					publisherBus.Logger.LogInformation($"[{publisherBus.Id}] Published MessageAa 1 {messageAa1}");

					Thread.Sleep(250);
					var messageAa2 = new MessageAa(1);
					publisherBus.ScopedBusControl.Publish<IMessageAa>(messageAa2);
					publisherBus.Logger.LogInformation($"[{publisherBus.Id}] Published MessageAa 2 {messageAa2}");

					Console.ReadKey();
				}
			}
		}

		private static void DemoScoped_HigherPriorityMessageAaConsumer1_LowerPriorityMessageAaConsumer2()
		{
			string idAa1 = $"{nameof(DemoScoped_HigherPriorityMessageAaConsumer1_LowerPriorityMessageAaConsumer2)}{nameof(MessageAaConsumer1)} 1";
			string idAa2 = $"{nameof(DemoScoped_HigherPriorityMessageAaConsumer1_LowerPriorityMessageAaConsumer2)}{nameof(MessageAaConsumer2)} 2";

			string queueAa = $"{nameof(DemoScoped_HigherPriorityMessageAaConsumer1_LowerPriorityMessageAaConsumer2)}{nameof(IMessageAa)}";

			int priorityAa1 = 2;
			int priorityAa2 = 1;

			ushort prefetchCountAa1 = 0;
			ushort prefetchCountAa2 = 0;

			using (var messageAConsumerBus1 =
				new MassTransitConsumer<IMessageAa, MessageAaConsumer1>(idAa1, queueAa, priorityAa1, prefetchCountAa1))
			using (var messageAConsumerBus2 =
				new MassTransitConsumer<IMessageAa, MessageAaConsumer2>(idAa2, queueAa, priorityAa2, prefetchCountAa2))
			{
				using (var publisherBus = new MassTransitPublisher($"{nameof(DemoScoped_MessageAaConsumer1_MessageAaConsumer2)}Publisher"))
				{
					Console.WriteLine("Press any clear and continue");
					Console.WriteLine("================================================");
					Console.ReadKey();
					Console.Clear();
					Console.WriteLine("================================================");
					Console.WriteLine("This demo has 2 consumers and 1 publisher");
					Console.WriteLine("Each runs its own bus, in its own DI scope");
					Console.WriteLine($"Publisher publishes 2 {nameof(IMessageAa)} messages");
					Console.WriteLine($"Both consumer takes {nameof(IMessageAa)} message but one has higher priority than other");
					Console.WriteLine("================================================");
					Console.WriteLine("Press any key start");
					Console.ReadKey();

					messageAConsumerBus1.ScopedBusControl.StartAsync(new CancellationToken());
					messageAConsumerBus1.Logger.LogInformation($"[{messageAConsumerBus1.Id}] Bus started in scope");

					messageAConsumerBus2.ScopedBusControl.StartAsync(new CancellationToken());
					messageAConsumerBus2.Logger.LogInformation($"[{messageAConsumerBus2.Id}] Bus started in scope");

					publisherBus.ScopedBusControl.Start();
					publisherBus.Logger.LogInformation($"[{publisherBus.Id}] Bus started in scope");

					var messageAa1 = new MessageAa(1);
					publisherBus.ScopedBusControl.Publish<IMessageAa>(messageAa1);
					publisherBus.Logger.LogInformation($"[{publisherBus.Id}] Published MessageAa 1 {messageAa1}");

					Thread.Sleep(250);
					var messageAa2 = new MessageAa(1);
					publisherBus.ScopedBusControl.Publish<IMessageAa>(messageAa2);
					publisherBus.Logger.LogInformation($"[{publisherBus.Id}] Published MessageAa 2 {messageAa2}");

					Console.ReadKey();
				}
			}
		}
	}
}
