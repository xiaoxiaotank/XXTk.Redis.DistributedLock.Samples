using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

#pragma warning disable CS1591 // 缺少对公共可见类型或成员的 XML 注释
namespace XXTk.Redis.DistributedLock.Api
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration((context, config) =>
                {
                    config.SetBasePath(context.HostingEnvironment.ContentRootPath)
                        .AddJsonFile("redis.json", optional: false, reloadOnChange: true)
                        .AddJsonFile($"redis.{context.HostingEnvironment.EnvironmentName}.json", optional: true, reloadOnChange: true)
                        .AddEnvironmentVariables();
                })
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseStartup<Startup>()
                        .UseUrls("http://*:5000");
                });
    }
}
#pragma warning restore CS1591 // 缺少对公共可见类型或成员的 XML 注释

