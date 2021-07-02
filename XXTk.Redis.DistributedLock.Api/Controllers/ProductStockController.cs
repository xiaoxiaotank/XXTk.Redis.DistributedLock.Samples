using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using RedLockNet;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Timers;

namespace XXTk.Redis.DistributedLock.Api.Controllers
{
    /// <summary>
    /// 库存
    /// </summary>
    [ApiController]
    [Route("[controller]")]
    public class ProductStockController : ControllerBase
    {
        private const int ProductId = 10000;
        private static readonly object _lockObj = new();

        private readonly ILogger<ProductStockController> _logger;
        private readonly IRedisCacheClient _redisCacheClient;
        private readonly IRedisDatabase _redisDatabase;
        private readonly IDistributedLockFactory _distributedLockFactory;

        /// <summary>
        /// 库存
        /// </summary>
        /// <param name="logger"></param>
        /// <param name="redisCacheClient"></param>
        /// <param name="redisDatabase"></param>
        /// <param name="distributedLockFactory"></param>
        public ProductStockController(
            ILogger<ProductStockController> logger,
            IRedisCacheClient redisCacheClient,
            IRedisDatabase redisDatabase,
            IDistributedLockFactory distributedLockFactory)
        {
            _logger = logger;
            _redisCacheClient = redisCacheClient;
            _redisDatabase = redisDatabase;
            _distributedLockFactory = distributedLockFactory;
        }

        /// <summary>
        /// 初始化库存
        /// </summary>
        /// <param name="quantity"></param>
        /// <returns></returns>
        [HttpPost("InitProductStock/{quantity:int?}")]
        public async Task InitProductStock([FromRoute] int quantity = 100)
        {
            await _redisDatabase.AddAsync(GetProductStockKey(ProductId), quantity);
        }

        /// <summary>
        /// 无锁扣减库存
        /// </summary>
        /// <returns></returns>
        [HttpPost("DecreaseProductStockWithNoLock")]
        public async Task<string> DecreaseProductStockWithNoLock()
        {
            var stockKey = GetProductStockKey(ProductId);
            var currentQuantity = (long)(await _redisDatabase.Database.StringGetAsync(stockKey));
            if (currentQuantity < 1)
                throw new Exception("库存不足");

            var leftQuantity = currentQuantity - 1;
            await _redisDatabase.Database.StringSetAsync(stockKey, leftQuantity);

            return $"剩余库存：{leftQuantity}";
        }

        /// <summary>
        /// 在单应用中扣减库存
        /// </summary>
        /// <returns></returns>
        [HttpPost("DecreaseProductStockInSingleApp")]
        public string DecreaseProductStockInSingleApp()
        {
            long leftQuantity;
            lock (_lockObj)
            {
                var stockKey = GetProductStockKey(ProductId);
                var currentQuantity = (long)_redisDatabase.Database.StringGet(stockKey);
                if (currentQuantity < 1)
                    throw new Exception("库存不足");

                leftQuantity = currentQuantity - 1;
                _redisDatabase.Database.StringSet(stockKey, leftQuantity);
            }

            return $"剩余库存：{leftQuantity}";
        }

        /// <summary>
        /// 在应用集群中扣减库存V1
        /// </summary>
        /// <returns></returns>
        [HttpPost("v1/DecreaseProductStockInAppCluster")]
        public async Task<string> DecreaseProductStockInAppClusterV1()
        {
            var lockKey = GetDistributedLockKey(ProductId.ToString());
            try
            {
                if(await _redisDatabase.AddAsync(lockKey, 1, When.NotExists))
                {
                    var stockKey = GetProductStockKey(ProductId);
                    var currentQuantity = (long)_redisDatabase.Database.StringGet(stockKey);
                    if (currentQuantity < 1)
                        throw new Exception("库存不足");

                    var leftQuantity = currentQuantity - 1;
                    _redisDatabase.Database.StringSet(stockKey, leftQuantity);

                    return $"剩余库存：{leftQuantity}";
                }
                else
                    throw new Exception("获取锁失败");
            }
            finally
            {
                _redisDatabase.Database.KeyDelete(lockKey);
            }
        }

        /// <summary>
        /// 在应用集群中扣减库存V2
        /// </summary>
        /// <returns></returns>
        [HttpPost("v2/DecreaseProductStockInAppCluster")]
        public async Task<string> DecreaseProductStockInAppClusterV2()
        {
            var lockKey = GetDistributedLockKey(ProductId.ToString());
            try
            {
                if (await _redisDatabase.AddAsync(lockKey, 1, TimeSpan.FromSeconds(30), When.NotExists))
                {
                    var stockKey = GetProductStockKey(ProductId);
                    var currentQuantity = (long)_redisDatabase.Database.StringGet(stockKey);
                    if (currentQuantity < 1)
                        throw new Exception("库存不足");

                    var leftQuantity = currentQuantity - 1;
                    _redisDatabase.Database.StringSet(stockKey, leftQuantity);

                    return $"剩余库存：{leftQuantity}";
                }
                else
                    throw new Exception("获取锁失败");
            }
            finally
            {
                _redisDatabase.Database.KeyDelete(lockKey);
            }
        }

        /// <summary>
        /// 在应用集群中扣减库存V3
        /// </summary>
        /// <returns></returns>
        [HttpPost("v3/DecreaseProductStockInAppCluster")]
        public async Task<string> DecreaseProductStockInAppClusterV3()
        {
            var lockKey = GetDistributedLockKey(ProductId.ToString());
            var resourceId = Guid.NewGuid().ToString();
            try
            {
                if (await _redisDatabase.AddAsync(lockKey, resourceId, TimeSpan.FromSeconds(30), When.NotExists))
                {
                    var stockKey = GetProductStockKey(ProductId);
                    var currentQuantity = (long)_redisDatabase.Database.StringGet(stockKey);
                    if (currentQuantity < 1)
                        throw new Exception("库存不足");

                    var leftQuantity = currentQuantity - 1;
                    _redisDatabase.Database.StringSet(stockKey, leftQuantity);

                    return $"剩余库存：{leftQuantity}";
                }
                else
                    throw new Exception("获取锁失败");
            }
            finally
            {
                if(await _redisDatabase.GetAsync<string>(lockKey) == resourceId)
                {
                    _redisDatabase.Database.KeyDelete(lockKey);
                }
            }
        }

        /// <summary>
        /// 在应用集群中扣减库存V4
        /// </summary>
        /// <returns></returns>
        [HttpPost("v4/DecreaseProductStockInAppCluster")]
        public async Task<string> DecreaseProductStockInAppClusterV4()
        {
            var lockKey = GetDistributedLockKey(ProductId.ToString());
            var resourceId = Guid.NewGuid().ToString();
            try
            {
                if (await _redisDatabase.AddAsync(lockKey, resourceId, TimeSpan.FromSeconds(30), When.NotExists))
                {
                    var stockKey = GetProductStockKey(ProductId);
                    var currentQuantity = (long)_redisDatabase.Database.StringGet(stockKey);
                    if (currentQuantity < 1)
                        throw new Exception("库存不足");

                    var leftQuantity = currentQuantity - 1;
                    _redisDatabase.Database.StringSet(stockKey, leftQuantity);

                    return $"剩余库存：{leftQuantity}";
                }
                else
                    throw new Exception("获取锁失败");
            }
            finally
            {
                await _redisDatabase.Database.ScriptEvaluateAsync(@"
                    if redis.call('get', KEYS[1]) == ARGV[1] then
    	                    return redis.call('del', KEYS[1])
                        else
    	                    return 0
                        end",
                keys: new RedisKey[] { lockKey },
                values: new RedisValue[] { resourceId });
            }
        }

        /// <summary>
        /// 在应用集群中扣减库存V5
        /// </summary>
        /// <returns></returns>
        [HttpPost("v5/DecreaseProductStockInAppCluster")]
        public async Task<string> DecreaseProductStockInAppClusterV5()
        {
            var lockKey = GetDistributedLockKey(ProductId.ToString());
            var resourceId = Guid.NewGuid().ToString();
            var expiresIn = TimeSpan.FromSeconds(30);
            try
            {
                if (await _redisDatabase.AddAsync(lockKey, resourceId, expiresIn, When.NotExists))
                {
                    var timer = new Timer()
                    {
                        Interval = expiresIn.TotalMilliseconds / 3,
                        Enabled = true,
                        AutoReset = true
                    };
                    timer.Elapsed += Timer_Elapsed;

                    var stockKey = GetProductStockKey(ProductId);
                    var currentQuantity = (long)_redisDatabase.Database.StringGet(stockKey);
                    if (currentQuantity < 1)
                        throw new Exception("库存不足");

                    var leftQuantity = currentQuantity - 1;
                    _redisDatabase.Database.StringSet(stockKey, leftQuantity);

                    return $"剩余库存：{leftQuantity}";
                }
                else
                    throw new Exception("获取锁失败");
            }
            finally
            {
                await _redisDatabase.Database.ScriptEvaluateAsync(@"
                    if redis.call('get', KEYS[1]) == ARGV[1] then
    	                    return redis.call('del', KEYS[1])
                        else
    	                    return 0
                        end",
                keys: new RedisKey[] { lockKey },
                values: new RedisValue[] { resourceId });
            }
        }

        private void Timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            
        }

        /// <summary>
        /// 通过使用RedLock在应用集群中扣减库存
        /// </summary>
        /// <returns></returns>
        [HttpPost("DecreaseProductStockInAppClusterWithRedLock")]
        public async Task<string> DecreaseProductStockInAppClusterWithRedLock()
        {
            using var redLock = await _distributedLockFactory.CreateLockAsync(
                resource: ProductId.ToString(),
                expiryTime: TimeSpan.FromSeconds(30),
                waitTime: TimeSpan.FromSeconds(30),
                retryTime: TimeSpan.FromSeconds(1)
            );

            if (redLock.IsAcquired)
            {
                var stockKey = GetProductStockKey(ProductId);
                var currentQuantity = (long)_redisDatabase.Database.StringGet(stockKey);
                if (currentQuantity < 1)
                    throw new Exception("库存不足");

                var leftQuantity = currentQuantity - 1;
                _redisDatabase.Database.StringSet(stockKey, leftQuantity);

                return $"剩余库存：{leftQuantity}";
            }
            else
                throw new Exception("获取锁失败");
        }

        [NonAction]
        private static string GetProductStockKey(int productId) => $"ProductStock_{productId}";

        [NonAction]
        private static string GetDistributedLockKey(string resource) => $"DistributedLock_{resource}";
    }
}
