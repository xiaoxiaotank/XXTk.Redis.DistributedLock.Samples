using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using RedLockNet;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
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

            // 使用 SETNX key value 命令加锁
            if (await _redisDatabase.AddAsync(lockKey, 1, When.NotExists, CommandFlags.DemandMaster))
            {
                try
                {
                    var stockKey = GetProductStockKey(ProductId);
                    var currentQuantity = (long)await _redisDatabase.Database.StringGetAsync(stockKey);
                    if (currentQuantity < 1)
                        throw new Exception("库存不足");

                    var leftQuantity = currentQuantity - 1;
                    await _redisDatabase.Database.StringSetAsync(stockKey, leftQuantity);

                    return $"剩余库存：{leftQuantity}";
                }
                finally
                {
                    // 释放锁
                    await _redisDatabase.Database.KeyDeleteAsync(lockKey, CommandFlags.DemandMaster);
                }
            }
            else
                throw new Exception("获取锁失败");
        }

        /// <summary>
        /// 在应用集群中扣减库存V2
        /// </summary>
        /// <returns></returns>
        [HttpPost("v2/DecreaseProductStockInAppCluster")]
        public async Task<string> DecreaseProductStockInAppClusterV2()
        {
            var lockKey = GetDistributedLockKey(ProductId.ToString());
            var expiresIn = TimeSpan.FromSeconds(30);

            // 使用 SET key value EX seconds NX 命令加锁，并设置过期时间
            if (await _redisDatabase.AddAsync(lockKey, 1, expiresIn, When.NotExists, CommandFlags.DemandMaster))
            {
                try
                {
                    var stockKey = GetProductStockKey(ProductId);
                    var currentQuantity = (long)await _redisDatabase.Database.StringGetAsync(stockKey);
                    if (currentQuantity < 1)
                        throw new Exception("库存不足");

                    var leftQuantity = currentQuantity - 1;
                    await _redisDatabase.Database.StringSetAsync(stockKey, leftQuantity);

                    return $"剩余库存：{leftQuantity}";
                }
                finally
                {
                    // 释放锁
                    await _redisDatabase.Database.KeyDeleteAsync(lockKey, CommandFlags.DemandMaster);
                }
            }
            else
                throw new Exception("获取锁失败");
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
            var expiresIn = TimeSpan.FromSeconds(30);

            // 使用 SET key value EX seconds NX 命令加锁，设置过期时间，并将值设置为业务Id
            if (await _redisDatabase.AddAsync(lockKey, resourceId, expiresIn, When.NotExists, CommandFlags.DemandMaster))
            {
                try
                {
                    var stockKey = GetProductStockKey(ProductId);
                    var currentQuantity = (long)await _redisDatabase.Database.StringGetAsync(stockKey);
                    if (currentQuantity < 1)
                        throw new Exception("库存不足");

                    var leftQuantity = currentQuantity - 1;
                    await _redisDatabase.Database.StringSetAsync(stockKey, leftQuantity);

                    return $"剩余库存：{leftQuantity}";
                }
                finally
                {
                    // 释放锁
                    if (await _redisDatabase.GetAsync<string>(lockKey) == resourceId)
                    {
                        _redisDatabase.Database.KeyDelete(lockKey, CommandFlags.DemandMaster);
                    }
                }
            }
            else
                throw new Exception("获取锁失败");
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
            var expiresIn = TimeSpan.FromSeconds(3000);

            // 使用 SET key value EX seconds NX 命令加锁，设置过期时间，并将值设置为业务Id
            if (await _redisDatabase.Database.StringSetAsync(lockKey, resourceId, expiresIn, When.NotExists, CommandFlags.DemandMaster))
            {
                try
                {
                    var stockKey = GetProductStockKey(ProductId);
                    var currentQuantity = (long)await _redisDatabase.Database.StringGetAsync(stockKey);
                    if (currentQuantity < 1)
                        throw new Exception("库存不足");

                    var leftQuantity = currentQuantity - 1;
                    await _redisDatabase.Database.StringSetAsync(stockKey, leftQuantity);

                    return $"剩余库存：{leftQuantity}";
                }
                finally
                {
                    // 释放锁，使用lua脚本实现操作的原子性
                    var result = await _redisDatabase.Database.ScriptEvaluateAsync(@"
                        if redis.call('get', KEYS[1]) == ARGV[1] then
    	                    return redis.call('del', KEYS[1])
                        else
    	                    return 20
                        end",
                     keys: new RedisKey[] { lockKey },
                     values: new RedisValue[] { resourceId },
                     CommandFlags.DemandMaster);

                }
            }
            else
                throw new Exception("获取锁失败");
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

            // 使用 SET key value EX seconds NX 命令加锁，设置过期时间，并将值设置为业务Id
            if (await _redisDatabase.AddAsync(lockKey, resourceId, expiresIn, When.NotExists, CommandFlags.DemandMaster))
            {
                try
                {
                    // 启动定时器，定时延长key的过期时间
                    var interval = expiresIn.TotalMilliseconds / 2;
                    var timer = new System.Threading.Timer(
                        callback: state => ExtendLockLifetime(lockKey, resourceId, expiresIn),
                        state: null,
                        dueTime: (int)interval,
                        period: (int)interval);

                    var stockKey = GetProductStockKey(ProductId);
                    var currentQuantity = (long)await _redisDatabase.Database.StringGetAsync(stockKey);
                    if (currentQuantity < 1)
                        throw new Exception("库存不足");

                    var leftQuantity = currentQuantity - 1;
                    await _redisDatabase.Database.StringSetAsync(stockKey, leftQuantity);

                    timer.Change(Timeout.Infinite, Timeout.Infinite);
                    timer.Dispose();
                    timer = null;

                    return $"剩余库存：{leftQuantity}";
                }
                finally
                {
                    // 释放锁，使用lua脚本实现操作的原子性
                    await _redisDatabase.Database.ScriptEvaluateAsync(@"
                        if redis.call('get', KEYS[1]) == ARGV[1] then
    	                    return redis.call('del', KEYS[1])
                        else
    	                    return 0
                        end",
                     keys: new RedisKey[] { lockKey },
                     values: new RedisValue[] { resourceId },
                     CommandFlags.DemandMaster);
                }
            }
            else
                throw new Exception("获取锁失败");
        }

        private void ExtendLockLifetime(string lockKey, string resourceId, TimeSpan expiresIn)
        {
            _redisDatabase.Database.ScriptEvaluate(@"
                local currentVal = redis.call('get', KEYS[1])
                if (currentVal == false) then
	                return redis.call('set', KEYS[1], ARGV[1], 'PX', ARGV[2]) and 1 or 0
                elseif (currentVal == ARGV[1]) then
	                return redis.call('pexpire', KEYS[1], ARGV[2])
                else
	                return -1
                end
            ",
            keys: new RedisKey[] { lockKey },
            values: new RedisValue[] { resourceId, (long)expiresIn.TotalMilliseconds },
            CommandFlags.DemandMaster);
        }

        /// <summary>
        /// 通过使用RedLock在应用集群中扣减库存
        /// </summary>
        /// <returns></returns>
        [HttpPost("DecreaseProductStockInAppClusterWithRedLock")]
        public async Task<string> DecreaseProductStockInAppClusterWithRedLock()
        {
            // 锁的过期时间为30s，等待获取锁的时间为20s，如果没有获取到锁，则等待1秒钟后再次尝试获取
            using var redLock = await _distributedLockFactory.CreateLockAsync(
                resource: ProductId.ToString(),
                expiryTime: TimeSpan.FromSeconds(30),
                waitTime: TimeSpan.FromSeconds(20),
                retryTime: TimeSpan.FromSeconds(1)
            );

            // 确认是否已获取到锁
            if (redLock.IsAcquired)
            {
                var stockKey = GetProductStockKey(ProductId);
                var currentQuantity = (long)await _redisDatabase.Database.StringGetAsync(stockKey);
                if (currentQuantity < 1)
                    throw new Exception("库存不足");

                var leftQuantity = currentQuantity - 1;
                await _redisDatabase.Database.StringSetAsync(stockKey, leftQuantity);

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
