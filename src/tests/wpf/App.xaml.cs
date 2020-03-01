using Serilog;
using Serilog.Sinks.Extensions;
using System.Windows;

namespace wpf_client
{
    /// <summary>
    /// Interaction logic for App.xaml
    /// </summary>
    public partial class App : Application
    {
        protected override void OnStartup(StartupEventArgs e)
        {
            base.OnStartup(e);

            Serilog.Log.Logger = new LoggerConfiguration()
                                .WriteTo.SQLLiteNET("serilog.db3")
                                .CreateLogger();


            Log.Information("test");
        }
    }
}
