using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MassTransit;
using MassTransit.ExtensionsDependencyInjectionIntegration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace MassTransitConsoleApp
{
	class Program
	{
		static void Main(string[] args)
		{
			DemoMessageAConsumerMessageBConsumer();
			Thread.Sleep(250);
			DemoMessageAaConsumer1MessageAaConsumer2();
			Thread.Sleep(250);
			DemoHigherPriorityMessageAaConsumer1LowerPriorityMessageAaConsumer2();
		}

		private static void DemoMessageAConsumerMessageBConsumer()
		{
			string idA = $"{nameof(DemoMessageAConsumerMessageBConsumer)}{nameof(MessageAConsumer)}";
			string idB = $"{nameof(DemoMessageAConsumerMessageBConsumer)}{nameof(MessageBConsumer)}";

			string queueA = $"{nameof(DemoMessageAConsumerMessageBConsumer)}{nameof(MessageAConsumer)}";
			string queueB = $"{nameof(DemoMessageAConsumerMessageBConsumer)}{nameof(MessageBConsumer)}";

			int priorityA = 1;
			int priorityB = 1;

			ushort prefetchCountA = 0;
			ushort prefetchCountB = 0;

			using (var messageAConsumerBus =
				new ScopedMassTransitConsumer<IMessageA, MessageAConsumer>(idA, queueA, priorityA, prefetchCountA))
			using (var messageBConsumerBus =
				new ScopedMassTransitConsumer<IMessageB, MessageBConsumer>(idB, queueB, priorityB, prefetchCountB))
			{
				using (var publisherBus = new ScopedMassTransitPublisher($"{nameof(DemoMessageAConsumerMessageBConsumer)} Publisher"))
				{
					Console.WriteLine("Press any key start");
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

		private static void DemoMessageAaConsumer1MessageAaConsumer2()
		{
			string idAa1 = $"{nameof(DemoMessageAaConsumer1MessageAaConsumer2)}{nameof(MessageAaConsumer1)} 1";
			string idAa2 = $"{nameof(DemoMessageAaConsumer1MessageAaConsumer2)}{nameof(MessageAaConsumer2)} 2";

			string queueAa = $"{nameof(DemoMessageAaConsumer1MessageAaConsumer2)}{nameof(IMessageAa)}";

			int priorityAa1 = 1;
			int priorityAa2 = 1;

			ushort prefetchCountAa1 = 0;
			ushort prefetchCountAa2 = 0;

			using (var messageAConsumerBus1 =
				new ScopedMassTransitConsumer<IMessageAa, MessageAaConsumer1>(idAa1, queueAa, priorityAa1, prefetchCountAa1))
			using (var messageAConsumerBus2 =
				new ScopedMassTransitConsumer<IMessageAa, MessageAaConsumer2>(idAa2, queueAa, priorityAa2, prefetchCountAa2))
			{
				using (var publisherBus = new ScopedMassTransitPublisher($"{nameof(DemoMessageAaConsumer1MessageAaConsumer2)}Publisher"))
				{
					Console.WriteLine("Press any key start");
					Console.ReadKey();

					messageAConsumerBus1.BusControl.StartAsync(new CancellationToken());
					messageAConsumerBus1.Logger.LogInformation($"[{messageAConsumerBus1.Id}] Bus started in scope");

					messageAConsumerBus2.BusControl.StartAsync(new CancellationToken());
					messageAConsumerBus2.Logger.LogInformation($"[{messageAConsumerBus2.Id}] Bus started in scope");

					publisherBus.BusControl.Start();
					publisherBus.Logger.LogInformation($"[{publisherBus.Id}] Bus started in scope");

					var messageAa1 = new MessageAa(1);
					publisherBus.BusControl.Publish<IMessageAa>(messageAa1);
					publisherBus.Logger.LogInformation($"[{publisherBus.Id}] Published MessageAa 1 {messageAa1}");

					Thread.Sleep(250);
					var messageAa2 = new MessageAa(1);
					publisherBus.BusControl.Publish<IMessageAa>(messageAa2);
					publisherBus.Logger.LogInformation($"[{publisherBus.Id}] Published MessageAa 2 {messageAa2}");

					Console.ReadKey();
				}
			}
		}

		private static void DemoHigherPriorityMessageAaConsumer1LowerPriorityMessageAaConsumer2()
		{
			string idAa1 = $"{nameof(DemoHigherPriorityMessageAaConsumer1LowerPriorityMessageAaConsumer2)}{nameof(MessageAaConsumer1)} 1";
			string idAa2 = $"{nameof(DemoHigherPriorityMessageAaConsumer1LowerPriorityMessageAaConsumer2)}{nameof(MessageAaConsumer2)} 2";

			string queueAa = $"{nameof(DemoHigherPriorityMessageAaConsumer1LowerPriorityMessageAaConsumer2)}{nameof(IMessageAa)}";

			int priorityAa1 = 2;
			int priorityAa2 = 1;

			ushort prefetchCountAa1 = 0;
			ushort prefetchCountAa2 = 0;

			using (var messageAConsumerBus1 =
				new ScopedMassTransitConsumer<IMessageAa, MessageAaConsumer1>(idAa1, queueAa, priorityAa1, prefetchCountAa1))
			using (var messageAConsumerBus2 =
				new ScopedMassTransitConsumer<IMessageAa, MessageAaConsumer2>(idAa2, queueAa, priorityAa2, prefetchCountAa2))
			{
				using (var publisherBus = new ScopedMassTransitPublisher($"{nameof(DemoMessageAaConsumer1MessageAaConsumer2)}Publisher"))
				{
					Console.WriteLine("Press any key start");
					Console.ReadKey();

					messageAConsumerBus1.BusControl.StartAsync(new CancellationToken());
					messageAConsumerBus1.Logger.LogInformation($"[{messageAConsumerBus1.Id}] Bus started in scope");

					messageAConsumerBus2.BusControl.StartAsync(new CancellationToken());
					messageAConsumerBus2.Logger.LogInformation($"[{messageAConsumerBus2.Id}] Bus started in scope");

					publisherBus.BusControl.Start();
					publisherBus.Logger.LogInformation($"[{publisherBus.Id}] Bus started in scope");

					var messageAa1 = new MessageAa(1);
					publisherBus.BusControl.Publish<IMessageAa>(messageAa1);
					publisherBus.Logger.LogInformation($"[{publisherBus.Id}] Published MessageAa 1 {messageAa1}");

					Thread.Sleep(250);
					var messageAa2 = new MessageAa(1);
					publisherBus.BusControl.Publish<IMessageAa>(messageAa2);
					publisherBus.Logger.LogInformation($"[{publisherBus.Id}] Published MessageAa 2 {messageAa2}");

					Console.ReadKey();
				}
			}
		}

		private static ServiceProvider BuildMassTransitServiceProvider<TMessage, TMessageConsumer>(IConfigurationRoot configurationRoot)
			where TMessage : class
			where TMessageConsumer : class, IConsumer<TMessage>
		{
			var services = new ServiceCollection()
				.AddLogging(configure =>
				{
					configure.AddConfiguration(configurationRoot.GetSection("Logging"));
					configure.AddConsole();
				});

			services.AddScoped<TMessageConsumer>();

			services.AddMassTransit(x =>
			{
				x.AddConsumer<TMessageConsumer>();
			});

			services.AddScoped<IBusControl>(provider => Bus.Factory.CreateUsingRabbitMq(cfg =>
			{
				var host = cfg.Host(new Uri("rabbitmq://localhost"), h =>
				{
					h.Username("guest");
					h.Password("guest");
				});

				string uniqueQueueName =
					configurationRoot.GetValue<string>("MassTransit:UniqueQueueName", "test_queue_b");
				cfg.ReceiveEndpoint(host, uniqueQueueName, ep =>
				{
					ep.ConsumerPriority = 2;
					ep.LoadFrom(provider);
					EndpointConvention.Map<TMessage>(ep.InputAddress);
				});

				cfg.UseExtensionsLogging(new LoggerFactory());
				cfg.UseExtensionsLogging(provider.GetRequiredService<ILoggerFactory>());
			}))
			.AddScoped<IBus>(provider => provider.GetRequiredService<IBusControl>());

			return services.BuildServiceProvider();
		}

		private static IConfigurationRoot BuildInMemoryConfiguration(string uniqueQueueName)
		{
			var configurationRoot = new ConfigurationBuilder()
				.AddInMemoryCollection(new Dictionary<string, string>()
				{
					{"Logging:LogLevel:Default", "Information"},
					{"Logging:Console:IncludeScopes", "True"},
					{"MassTransit:UniqueQueueName", uniqueQueueName}
			})
				.Build();
			return configurationRoot;
		}
	}

	public class ScopedMassTransitPublisher : IDisposable
	{
		public IBusControl BusControl { get; set; }
		public ILogger Logger { get; set; }
		public ServiceProvider ServiceProvider { get; set; }
		public IServiceScopeFactory ServiceScopeFactory { get; set; }
		public IServiceScope ServiceScope { get; set; }
		public IConfigurationRoot ConfigurationRoot { get; set; }
		public string Id { get; set; }

		public ScopedMassTransitPublisher(string id)
		{
			Id = id;
			ConfigurationRoot = BuildInMemoryConfiguration();
			ServiceProvider = BuildMassTransitPublisher(ConfigurationRoot);
			ServiceScopeFactory = ServiceProvider.GetService<IServiceScopeFactory>();
			ServiceScope = ServiceScopeFactory.CreateScope();
			BusControl = ServiceScope.ServiceProvider.GetService<IBusControl>();
			Logger = ServiceProvider.GetService<ILoggerFactory>().CreateLogger<ScopedMassTransitPublisher>();
		}

		private static IConfigurationRoot BuildInMemoryConfiguration()
		{
			var configurationRoot = new ConfigurationBuilder()
				.AddInMemoryCollection(new Dictionary<string, string>()
				{
					{"Logging:LogLevel:Default", "Information"},
					{"Logging:Console:IncludeScopes", "True"},
				})
				.Build();
			return configurationRoot;
		}


		private static ServiceProvider BuildMassTransitPublisher(IConfigurationRoot configurationRoot)
		{
			var services = new ServiceCollection()
				.AddLogging(configure =>
				{
					configure.AddConfiguration(configurationRoot.GetSection("Logging"));
					configure.AddConsole();
				});

			services.AddScoped<IBusControl>(provider => Bus.Factory.CreateUsingRabbitMq(cfg =>
				{
					var host = cfg.Host(new Uri("rabbitmq://localhost"), h =>
					{
						h.Username("guest");
						h.Password("guest");
					});
					cfg.AutoDelete = true;
					cfg.Durable = false;
					cfg.UseExtensionsLogging(new LoggerFactory());
					cfg.UseExtensionsLogging(provider.GetRequiredService<ILoggerFactory>());
				}))
				.AddScoped<IBus>(provider => provider.GetRequiredService<IBusControl>());
			return services.BuildServiceProvider();
		}

		public void Dispose()
		{
			BusControl.Stop();
			ServiceScope?.Dispose();
			Logger.LogInformation($"{Id} Bus stopped, scope disposed");
		}
	}

	public class ScopedMassTransitConsumer<TMessage, TMessageConsumer> : IDisposable
		where TMessage : class
		where TMessageConsumer : class, IConsumer<TMessage>
	{
		public IBusControl BusControl { get; set; }
		public ILogger Logger { get; set; }
		public ServiceProvider ServiceProvider { get; set; }
		public IServiceScopeFactory ServiceScopeFactory { get; set; }
		public IServiceScope ServiceScope { get; set; }
		public IConfigurationRoot ConfigurationRoot { get; set; }
		public string Id { get; set; }
		public string UniqueQueueName { get; set; }

		public ScopedMassTransitConsumer(string id, string uniqueQueueName, int consumerPriority, ushort prefetchCount)
		{
			Id = id;
			UniqueQueueName = uniqueQueueName;
			ConfigurationRoot = BuildInMemoryConfiguration();
			ServiceProvider = BuildMassTransitConsumer(ConfigurationRoot, uniqueQueueName, consumerPriority, prefetchCount);
			ServiceScopeFactory = ServiceProvider.GetService<IServiceScopeFactory>();
			ServiceScope = ServiceScopeFactory.CreateScope();
			BusControl = ServiceScope.ServiceProvider.GetService<IBusControl>();
			Logger = ServiceProvider.GetService<ILoggerFactory>().CreateLogger<ScopedMassTransitConsumer<TMessage, TMessageConsumer>>();
		}

		private static IConfigurationRoot BuildInMemoryConfiguration()
		{
			var configurationRoot = new ConfigurationBuilder()
				.AddInMemoryCollection(new Dictionary<string, string>()
				{
					{"Logging:LogLevel:Default", "Information"},
					{"Logging:Console:IncludeScopes", "True"},
				})
				.Build();
			return configurationRoot;
		}


		private static ServiceProvider BuildMassTransitConsumer(IConfigurationRoot configurationRoot, string uniqueQueueName, int consumerPriority, ushort prefetchCount)
		{
			var services = new ServiceCollection()
				.AddLogging(configure =>
				{
					configure.AddConfiguration(configurationRoot.GetSection("Logging"));
					configure.AddConsole();
				});

			services.AddScoped<TMessageConsumer>();

			services.AddMassTransit(x =>
			{
				x.AddConsumer<TMessageConsumer>();
			});

			services.AddScoped<IBusControl>(provider => Bus.Factory.CreateUsingRabbitMq(cfg =>
				{
					var host = cfg.Host(new Uri("rabbitmq://localhost"), h =>
					{
						h.Username("guest");
						h.Password("guest");
					});
					cfg.AutoDelete = true;
					cfg.Durable = false;

					cfg.ReceiveEndpoint(host, uniqueQueueName, ep =>
					{
						ep.ConsumerPriority = consumerPriority;
						ep.PrefetchCount = prefetchCount;
						ep.LoadFrom(provider);
						ep.AutoDelete = true;
						ep.Durable = false;
						ep.PurgeOnStartup = true;
						EndpointConvention.Map<TMessage>(ep.InputAddress);
					});

					cfg.UseExtensionsLogging(new LoggerFactory());
					cfg.UseExtensionsLogging(provider.GetRequiredService<ILoggerFactory>());
				}))
				.AddScoped<IBus>(provider => provider.GetRequiredService<IBusControl>());

			return services.BuildServiceProvider();
		}

		public void Dispose()
		{
			BusControl.Stop();
			ServiceScope?.Dispose();
			Logger.LogInformation($"{Id} Bus stopped, scope disposed");
		}
	}

	public interface IMessageAa
	{
		string Value { get; }
		int SecondsToSleep { get; }
	}

	public interface IMessageA
	{
		string Value { get; }
		int SecondsToSleep { get; }
	}
	public interface IMessageB
	{
		string Value { get; }
		int SecondsToSleep { get; }
	}

	public class MessageAa : IMessageAa
	{
		public string Value { get; set; } = DateTime.Now.ToString("dd-mmm-yyyy h:m:s.fff");
		public int SecondsToSleep { get; set; }

		public MessageAa(int secondsToSleep)
		{
			SecondsToSleep = secondsToSleep;
		}

		public override string ToString()
		{
			return $"{Value} {SecondsToSleep}";
		}
	}

	public class MessageA : IMessageA
	{
		public string Value { get; set; } = DateTime.Now.ToString("dd-mmm-yyyy h:m:s.fff");
		public int SecondsToSleep { get; set; }

		public MessageA(int secondsToSleep)
		{
			SecondsToSleep = secondsToSleep;
		}

		public override string ToString()
		{
			return $"{Value} {SecondsToSleep}";
		}
	}

	public class MessageB : IMessageB
	{
		public string Value { get; set; } = DateTime.Now.ToString("dd-mmm-yyyy h:m:s.fff");
		public int SecondsToSleep { get; set; }

		public MessageB(int secondsToSleep)
		{
			SecondsToSleep = secondsToSleep;
		}
		public override string ToString()
		{
			return $"{Value} {SecondsToSleep}";
		}
	}

	public class MessageAaConsumer1 : IConsumer<IMessageAa>
	{
		public Task Consume(ConsumeContext<IMessageAa> context)
		{
			Console.Out.WriteLine($"Received by {nameof(MessageAaConsumer1)}: {context.Message.Value} {context.Message.SecondsToSleep}");
			Thread.Sleep(context.Message.SecondsToSleep * 1000);
			Console.Out.WriteLine($"Received by {nameof(MessageAaConsumer1)}: Done");
			return Task.CompletedTask;
		}
	}

	public class MessageAaConsumer2 : IConsumer<IMessageAa>
	{
		public Task Consume(ConsumeContext<IMessageAa> context)
		{
			Console.Out.WriteLine($"Received by {nameof(MessageAaConsumer2)}: {context.Message.Value} {context.Message.SecondsToSleep}");
			Thread.Sleep(context.Message.SecondsToSleep * 1000);
			Console.Out.WriteLine($"Received by {nameof(MessageAaConsumer2)}: Done");
			return Task.CompletedTask;
		}
	}

	public class MessageAConsumer : IConsumer<IMessageA>
	{
		public Task Consume(ConsumeContext<IMessageA> context)
		{
			Console.Out.WriteLine($"Received {nameof(IMessageA)}: {context.Message.Value} {context.Message.SecondsToSleep}");
			Thread.Sleep(context.Message.SecondsToSleep * 1000);
			Console.Out.WriteLine($"Received {nameof(IMessageA)}: Done");
			return Task.CompletedTask;
		}
	}

	public class MessageBConsumer : IConsumer<IMessageB>
	{
		public Task Consume(ConsumeContext<IMessageB> context)
		{
			Console.Out.WriteLine($"Received {nameof(IMessageB)}: {context.Message.Value} {context.Message.SecondsToSleep}");
			Thread.Sleep(context.Message.SecondsToSleep * 1000);
			Console.Out.WriteLine($"Received {nameof(IMessageB)}: Done");
			return Task.CompletedTask;
		}
	}
}
