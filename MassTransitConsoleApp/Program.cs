using System;
using System.Collections.Generic;
using MassTransit;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace MassTransitConsoleApp
{
	class Program
	{
		static void Main(string[] args)
		{
			if (args == null)
			{

			}
			DemoMassTransitConfigurationA();
			DemoMassTransitConfigurationB();
		}

		private static void DemoMassTransitConfigurationB()
		{
			var configurationRoot = BuildInMemoryConfiguration("ConfigurationB");
			var serviceProvider = BuildMassTransitServiceProviderForTestQueueB(configurationRoot);
			var serviceScopeFactory = serviceProvider.GetService<IServiceScopeFactory>();
			var logger = GetLogger(serviceProvider);
			IBusControl busControlScope2;
			using (var serviceScope2 = serviceScopeFactory.CreateScope())
			{
				logger.LogInformation("==========scope2=========");
				busControlScope2 = serviceScope2.ServiceProvider.GetService<IBusControl>();
				busControlScope2.Start();
				busControlScope2.Publish<IMessageB>(new { Value = "Hi MessageA" });
			}

			logger.LogInformation("Press any key to stop scope2 bus");
			Console.ReadKey();
			busControlScope2.Stop();
		}

		private static void DemoMassTransitConfigurationA()
		{
			var configurationRoot = BuildInMemoryConfiguration("ConfigurationA");
			var serviceProvider = BuildMassTransitServiceProviderForTestQueueA(configurationRoot);
			var serviceScopeFactory = serviceProvider.GetService<IServiceScopeFactory>();
			var logger = GetLogger(serviceProvider);
			IBusControl busControlScope1;
			using (var serviceScope1 = serviceScopeFactory.CreateScope())
			{
				logger.LogInformation("==========scope1=========");
				busControlScope1 = serviceScope1.ServiceProvider.GetService<IBusControl>();
				busControlScope1.Start();
				busControlScope1.Publish<IMessageA>(new { Value = "Hi MessageB" });
			}

			logger.LogInformation("Press any key to stop scope1 bus");
			Console.ReadKey();
			busControlScope1.Stop();
		}

		private static ILogger<Program> GetLogger(ServiceProvider serviceProvider)
		{
			var logger = serviceProvider.GetService<ILoggerFactory>()
				.CreateLogger<Program>();
			return logger;
		}

		private static ServiceProvider BuildMassTransitServiceProviderForTestQueueA(IConfigurationRoot configurationRoot)
		{
			return new ServiceCollection()
				.AddLogging(configure =>
				{
					configure.AddConfiguration(configurationRoot.GetSection("Logging"));
					configure.AddConsole();
				})
				.AddScoped<IBusControl>(provider => Bus.Factory.CreateUsingRabbitMq(cfg =>
				{
					var host = cfg.Host(new Uri("rabbitmq://localhost"), h =>
					{
						h.Username("guest");
						h.Password("guest");
					});

					string uniqueQueueName = configurationRoot.GetValue<string>("MassTransit:UniqueQueueName");
					cfg.ReceiveEndpoint(host, uniqueQueueName, ep =>
					{
						ep.ConsumerPriority = 1;
						ep.Handler<IMessageA>(context => Console.Out.WriteLineAsync($"{uniqueQueueName}: {context.Message.Value}"));
					});

					cfg.UseExtensionsLogging(new LoggerFactory());
					cfg.UseExtensionsLogging(provider.GetRequiredService<ILoggerFactory>());
				}))
				.AddScoped<IBus>(provider => provider.GetRequiredService<IBusControl>())
				.BuildServiceProvider();
		}

		private static ServiceProvider BuildMassTransitServiceProviderForTestQueueB(IConfigurationRoot configurationRoot)
		{
			return new ServiceCollection()
				.AddLogging(configure =>
				{
					configure.AddConfiguration(configurationRoot.GetSection("Logging"));
					configure.AddConsole();
				})
				.AddScoped<IBusControl>(provider => Bus.Factory.CreateUsingRabbitMq(cfg =>
				{
					var host = cfg.Host(new Uri("rabbitmq://localhost"), h =>
					{
						h.Username("guest");
						h.Password("guest");
					});

					cfg.ReceiveEndpoint(host, "test_queue_b", ep =>
					{
						ep.ConsumerPriority = 2;
						ep.Handler<IMessageB>(context => Console.Out.WriteLineAsync($"Received via test_queue_b: {context.Message.Value}"));
					});

					cfg.UseExtensionsLogging(new LoggerFactory());
					cfg.UseExtensionsLogging(provider.GetRequiredService<ILoggerFactory>());
				}))
				.AddScoped<IBus>(provider => provider.GetRequiredService<IBusControl>())
				.BuildServiceProvider();
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

	public interface IMessageA { string Value { get; } }
	public interface IMessageB { string Value { get; } }
}
