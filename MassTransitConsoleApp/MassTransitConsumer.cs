using System;
using System.Collections.Generic;
using MassTransit;
using MassTransit.ExtensionsDependencyInjectionIntegration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace MassTransitConsoleApp
{
	public class MassTransitConsumer<TMessage, TMessageConsumer> : IDisposable
		where TMessage : class
		where TMessageConsumer : class, IConsumer<TMessage>
	{
		public IBusControl BusControl { get; set; }
		public IBusControl ScopedBusControl { get; set; }
		public ILogger Logger { get; set; }
		public ServiceProvider ServiceProvider { get; set; }
		public IServiceScopeFactory ServiceScopeFactory { get; set; }
		public IServiceScope ServiceScope { get; set; }
		public IConfigurationRoot ConfigurationRoot { get; set; }
		public string Id { get; set; }
		public string UniqueQueueName { get; set; }

		public MassTransitConsumer(string id, string uniqueQueueName, int consumerPriority, ushort prefetchCount)
		{
			Id = id;
			UniqueQueueName = uniqueQueueName;
			ConfigurationRoot = BuildInMemoryConfiguration();
			ServiceProvider = BuildMassTransitConsumer(ConfigurationRoot, uniqueQueueName, consumerPriority, prefetchCount);
			ServiceScopeFactory = ServiceProvider.GetService<IServiceScopeFactory>();
			ServiceScope = ServiceScopeFactory.CreateScope();
			ScopedBusControl = ServiceScope.ServiceProvider.GetService<IBusControl>();
			BusControl = ServiceProvider.GetService<IBusControl>();
			Logger = ServiceProvider.GetService<ILoggerFactory>().CreateLogger<MassTransitConsumer<TMessage, TMessageConsumer>>();
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
					configure.AddDebug();
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
			ScopedBusControl?.StopAsync();
			BusControl?.StopAsync();
			ServiceScope?.Dispose();
			Logger.LogInformation($"{Id} Bus stopped, scope disposed");
		}
	}
}
