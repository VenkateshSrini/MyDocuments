using StackExchange.Redis;
using Newtonsoft.Json;
using Polly;
using Polly.Retry;
public class RedisCacheWithException
{
    private readonly ConnectionMultiplexer _redis;
    private readonly IDatabase _db;
    private readonly Policy _retryPolicy;

    public RedisCacheWithException(string connectionString)
    {
        _redis = ConnectionMultiplexer.Connect(connectionString);
        _db = _redis.GetDatabase();

        _retryPolicy = Policy.Handle<Exception>()
            .WaitAndRetry(10, retryAttempt =>
            meSpan.FromSeconds(Math.Min(Math.Pow(2, retryAttempt), 120)));
    }

    public T GetOrSet<T>(string key, Func<T> fetch, TimeSpan expiry)
    {
        var value = _db.StringGet(key);

        if (!value.IsNull)
        {
            return JsonConvert.DeserializeObject<T>(value);
        }

        // Acquire a lock
        var lockToken = Guid.NewGuid();
        if (_db.LockTake(key, lockToken.ToString(), TimeSpan.FromMinutes(1)))
        {
            try
            {
                // Fetch the data
                var data = fetch();

                // Set the data in the cache
                _db.StringSet(key, JsonConvert.SerializeObject(data), expiry);

                return data;
            }
            finally
            {
                // Release the lock
                _db.LockRelease(key, lockToken.ToString());
            }
        }

        // If we couldn't acquire the lock, use Polly to retry
        return _retryPolicy.Execute(() =>
        {
            value = _db.StringGet(key);

            if (!value.IsNull)
            {
                return JsonConvert.DeserializeObject<T>(value);
            }

            throw new Exception("Value not found in cache");
        });
    }
}
