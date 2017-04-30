using System;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Utility;

#if TRACE
// alias needed due to Microsoft.SPOT.Trace in .Net Micro Framework
// (it's ambiguos with uPLibrary.Networking.M2Mqtt.Utility.Trace)
using MqttUtility = uPLibrary.Networking.M2Mqtt.Utility;
#endif

namespace GnatMQServer
{
	class Program
	{
		static void Main(string[] args)
		{
#if TRACE
			//MqttUtility.Trace.TraceLevel = MqttUtility.TraceLevel.Verbose | MqttUtility.TraceLevel.Frame;
			Trace.TraceLevel = (MqttUtility.TraceLevel)127;
			Trace.TraceListener = (f, a) =>
			Console.WriteLine(System.String.Format(f, a)
			);
#endif
			AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;

			// create and start broker
			MqttBroker broker = new MqttBroker();
			broker.Start();

			Console.ReadLine();

			broker.Stop();
		}

		private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
		{
#if TRACE
			Trace.WriteLine(TraceLevel.Error, "Unhandled exception occurred: " + e.ExceptionObject);
#endif
		}
	}
}
