import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;

// Obtain the JCache CachingProvider. Infinispan is automatically
// discovered when infinispan-jcache is on the classpath.
CachingProvider cachingProvider = Caching.getCachingProvider();
CacheManager cacheManager = cachingProvider.getCacheManager();

// Configure a cache with key and value types.
MutableConfiguration<String, String> configuration = new MutableConfiguration<>();
configuration.setTypes(String.class, String.class);

// Create a named cache with the configuration.
Cache<String, String> cache = cacheManager.createCache("myCache", configuration);
