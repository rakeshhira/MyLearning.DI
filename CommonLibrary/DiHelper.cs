using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ConsoleApp
{
	public static class DiHelper
	{
		public static ILogger<TCategory> GetLogger<TCategory>(ServiceProvider serviceProvider)
		{
			return serviceProvider.GetService<ILoggerFactory>().CreateLogger<TCategory>();
		}

		private static ServiceProvider BuildServiceProvider(IConfigurationRoot configurationRoot)
		{
			var serviceProvider = new ServiceCollection()
				.AddLogging(configure =>
				{
					configure.AddConfiguration(configurationRoot.GetSection("Logging"));
					configure.AddConsole();
					configure.AddDebug();
				})
				.AddSingleton<IServiceSingleton, ServiceSingleton>()
				.AddTransient<IServiceTransient, ServiceTransient>()
				.AddScoped<IServiceScoped, ServiceScoped>()
				.BuildServiceProvider();
			return serviceProvider;
		}

		private static IConfigurationRoot BuildInMemoryConfiguration()
		{
			var configurationRoot = new ConfigurationBuilder()
				.AddInMemoryCollection(new Dictionary<string, string>()
				{
					{"Logging:LogLevel:Default", "Information"},
					{"Logging:Console:IncludeScopes", "True"}
				})
				.Build();
			return configurationRoot;
		}
	}

	interface IService
	{
		DateTime CreateTime { get; }
		Guid Id { get; }
	}

	interface IServiceSingleton : IService { }
	interface IServiceTransient : IService { }
	interface IServiceScoped : IService { }

	class ServiceScoped : ServiceBase, IServiceScoped
	{
		public ServiceScoped(ILogger<ServiceScoped> logger)
			: base(logger)
		{
		}

		public override string ToString()
		{
			return $"{nameof(ServiceScoped)}> {base.ToString()}";
		}
	}

	class ServiceTransient : ServiceBase, IServiceTransient
	{
		public ServiceTransient(ILogger<ServiceTransient> logger)
			: base(logger)
		{
		}

		public override string ToString()
		{
			return $"{nameof(ServiceTransient)}> {base.ToString()}";
		}
	}

	class ServiceSingleton : ServiceBase, IServiceSingleton
	{
		public ServiceSingleton(ILogger<ServiceSingleton> logger)
			: base(logger)
		{
		}

		public override string ToString()
		{
			return $"{nameof(ServiceSingleton)}> {base.ToString()}";
		}
	}

	class ServiceBase : IService
	{
		private static readonly Func<ILogger, IDisposable> BeginScopeCtorLogger;
		private static readonly Action<ILogger, DateTime, Guid, Exception> LogDump;

		private readonly ILogger _logger;
		public DateTime CreateTime { get; set; }

		public Guid Id { get; set; }

		static ServiceBase()
		{
			BeginScopeCtorLogger = LoggerMessage.DefineScope("ctor(logger)");
			LogDump = LoggerMessage.Define<DateTime, Guid>(
				LogLevel.Trace,
				new EventId(1, nameof(ToString)),
				"[CreateTime={CreateTime}, Id={Id}]");
		}

		public ServiceBase(ILogger logger)
		{
			Thread.Sleep(1000);
			using (BeginScopeCtorLogger(logger))
			{
				_logger = logger;
				CreateTime = DateTime.Now;
				Id = Guid.NewGuid();
				LogDump(_logger, CreateTime, Id, null);
			}
		}

		public override string ToString()
		{
			return $"[CreateTime={CreateTime}, Id={Id}]";
		}
	}
}
