using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using RedLockNet;
using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
using StackExchange.Redis.Extensions.Core.Abstractions;
using StackExchange.Redis.Extensions.Core.Configuration;
using StackExchange.Redis.Extensions.Newtonsoft;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

#pragma warning disable CS1591 // 缺少对公共可见类型或成员的 XML 注释
namespace XXTk.Redis.DistributedLock.Api
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            // 请先在 redis.json 中修改为自己的配置
            var redisConfiguration = Configuration.GetSection("Redis").Get<RedisConfiguration>();
            services.AddStackExchangeRedisExtensions<NewtonsoftSerializer>(redisConfiguration);
            services.AddSingleton<IDistributedLockFactory>(provider =>
            {
                var redisCacheConnectionpoolManager = provider.GetRequiredService<IRedisCacheConnectionPoolManager>();
                var redLockFactory = RedLockFactory.Create(
                    new List<RedLockMultiplexer> { new RedLockMultiplexer(redisCacheConnectionpoolManager.GetConnection()) }
                );
                return redLockFactory;
            });

            services.AddControllers();
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo { Title = "XXTk.Redis.DistributedLock.Api", Version = "v1" });
                c.IncludeXmlComments("XXTk.Redis.DistributedLock.Api.xml", includeControllerXmlComments: true);
                c.DescribeAllParametersInCamelCase();
            });
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
                app.UseSwagger();
                app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "XXTk.Redis.DistributedLock.Api v1"));
                // 若Redis服务不可用，应用启动时会被阻塞
                //app.UseRedisInformation();
            }

            app.UseRouting();

            app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }
    }
}
#pragma warning restore CS1591 // 缺少对公共可见类型或成员的 XML 注释
