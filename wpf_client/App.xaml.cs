using Serilog;
using Serilog.Core;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
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
