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
