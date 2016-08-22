using System;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;

namespace Pomelo.EntityFrameworkCore.MyCat.Proxy
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var host = new WebHostBuilder()
                .UseKestrel()
                .UseUrls("http://*:7066")
                .UseContentRoot(Directory.GetCurrentDirectory())
                .UseIISIntegration()
                .UseStartup<Startup>()
                .Build();

            StartMyCat();
            host.Run();
        }

        public static void StartMyCat()
        {
            Process startProcess;
            if (System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows))
                startProcess = Process.Start(Path.Combine(Startup.MyCatRoot, "bin", "mycat.bat"), "start");
            else
                startProcess = Process.Start(Path.Combine(Startup.MyCatRoot, "bin", "mycat"), "start");
        }
    }
}
