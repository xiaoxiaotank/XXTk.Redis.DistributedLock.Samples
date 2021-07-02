using Newtonsoft.Json;
using StackExchange.Redis.Extensions.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace XXTk.Redis.DistributedLock.Api
{
    /// <summary>
    /// Redis对象序列化器
    /// </summary>
    /// <remarks>refer to StackExchange.Redis.Extensions.Newtonsoft </remarks>
    public class RedisNewtonsoftSerializer : ISerializer
    {
        /// <summary>
        /// Encoding to use to convert string to byte[] and the other way around.
        /// </summary>
        /// <remarks>
        /// StackExchange.Redis uses Encoding.UTF8 to convert strings to bytes,
        /// hence we do same here.
        /// </remarks>
        private static readonly Encoding encoding = Encoding.UTF8;

        private readonly JsonSerializerSettings settings;

        /// <summary>
        /// Initializes a new instance of the <see cref="RedisNewtonsoftSerializer"/> class.
        /// </summary>
        public RedisNewtonsoftSerializer()
            : this(null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RedisNewtonsoftSerializer"/> class.
        /// </summary>
        /// <param name="settings">The settings.</param>
        public RedisNewtonsoftSerializer(JsonSerializerSettings settings)
        {
            this.settings = settings ?? new JsonSerializerSettings();
        }

        /// <inheritdoc/>
        public byte[] Serialize(object item)
        {
            // 若 item 是字符串对象，StackExchange.Redis.Extensions.Newtonsoft 中的
            // NewtonsoftSerializer 会将引号（"）也进行转义（\"），使得数据存储时看起来很奇怪
            // 所以在此处修改为若对象是字符串，则无须进行Json序列化
            if (item is string itemString)
            {
                return encoding.GetBytes(itemString);
            }
            else
            {
                var type = item?.GetType();
                var jsonString = JsonConvert.SerializeObject(item, type, settings);
                return encoding.GetBytes(jsonString);
            }
        }

        /// <inheritdoc/>
        public T Deserialize<T>(byte[] serializedObject)
        {
            var jsonString = encoding.GetString(serializedObject);
            return JsonConvert.DeserializeObject<T>(jsonString, settings);
        }
    }
}
