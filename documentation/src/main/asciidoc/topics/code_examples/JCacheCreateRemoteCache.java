import javax.cache.*;
import javax.cache.configuration.*;

// Retrieve the system wide Cache Manager via org.infinispan.jcache.remote.JCachingProvider
CacheManager cacheManager = Caching.getCachingProvider("org.infinispan.jcache.remote.JCachingProvider").getCacheManager();
// Define a named cache with default JCache configuration
Cache<String, String> cache = cacheManager.createCache("remoteNamedCache",
      new MutableConfiguration<String, String>());
