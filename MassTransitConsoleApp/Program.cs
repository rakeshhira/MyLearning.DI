using System;
using System.Collections.Generic;
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
			DemoMassTransit<IMessageA, MessageAConsumer>(nameof(IMessageA), new MessageA());
			DemoMassTransit<IMessageB, MessageBConsumer>(nameof(IMessageB), new MessageB());
		}

		private static void DemoMassTransit<TMessage, TMessageConsumer>(string uniqueQueueName, TMessage message)
			where TMessage : class
			where TMessageConsumer : class, IConsumer<TMessage>
		{
			var configurationRoot = BuildInMemoryConfiguration(uniqueQueueName);
			var serviceProvider = BuildMassTransitServiceProvider<TMessage, TMessageConsumer>(configurationRoot);
			var logger = serviceProvider.GetService<ILoggerFactory>().CreateLogger<Program>();

			var serviceScopeFactory = serviceProvider.GetService<IServiceScopeFactory>();
			using (var serviceScope2 = serviceScopeFactory.CreateScope())
			{
				var busControl = serviceScope2.ServiceProvider.GetService<IBusControl>();
				busControl.Start();
				busControl.Publish<TMessage>(message);
				logger.LogInformation($"Published {message}");
				Console.ReadKey();
				busControl.Stop();
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

	public interface IMessageA
	{
		string Value { get; }
	}
	public interface IMessageB
	{
		string Value { get; }
	}

	public class MessageA : IMessageA
	{
		public string Value { get; set; } = DateTime.Now.ToLongTimeString();
		public override string ToString()
		{
			return Value;
		}
	}

	public class MessageB : IMessageB
	{
		public string Value { get; set; } = DateTime.Now.ToLongTimeString();
		public override string ToString()
		{
			return Value;
		}
	}

	public class MessageAConsumer : IConsumer<IMessageA>
	{
		public Task Consume(ConsumeContext<IMessageA> context)
		{
			Console.Out.WriteLine($"Received MessageA: {context.Message.Value}");
			return Task.CompletedTask;
		}
	}

	public class MessageBConsumer : IConsumer<IMessageB>
	{
		public Task Consume(ConsumeContext<IMessageB> context)
		{
			Console.Out.WriteLine($"Received MessageB: {context.Message.Value}");
			return Task.CompletedTask;
		}
	}
}
