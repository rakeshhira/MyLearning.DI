using System;
using System.Collections.Generic;
using MassTransit;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace MassTransitConsoleApp
{
	public class MassTransitPublisher : IDisposable
	{
		public IBusControl BusControl { get; set; }
		public IBusControl ScopedBusControl { get; set; }
		public ILogger Logger { get; set; }
		public ServiceProvider ServiceProvider { get; set; }
		public IServiceScopeFactory ServiceScopeFactory { get; set; }
		public IServiceScope ServiceScope { get; set; }
		public IConfigurationRoot ConfigurationRoot { get; set; }
		public string Id { get; set; }

		public MassTransitPublisher(string id)
		{
			Id = id;
			ConfigurationRoot = BuildInMemoryConfiguration();
			ServiceProvider = BuildMassTransitPublisher(ConfigurationRoot);
			ServiceScopeFactory = ServiceProvider.GetService<IServiceScopeFactory>();
			ServiceScope = ServiceScopeFactory.CreateScope();
			ScopedBusControl = ServiceScope.ServiceProvider.GetService<IBusControl>();
			BusControl = ServiceProvider.GetService<IBusControl>();
			Logger = ServiceProvider.GetService<ILoggerFactory>().CreateLogger<MassTransitPublisher>();
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
					configure.AddDebug();
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
			ScopedBusControl?.StopAsync();
			BusControl?.StopAsync();
			ServiceScope?.Dispose();
			Logger.LogInformation($"{Id} Bus stopped, scope disposed");
		}
	}
}
