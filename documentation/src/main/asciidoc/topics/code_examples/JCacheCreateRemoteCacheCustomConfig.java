import javax.cache.*;
import javax.cache.configuration.*;

// Load configuration from an absolute filesystem path
URI uri = URI.create("file:///path/to/hotrod-client.properties");
// Load configuration from a classpath resource
// URI uri = this.getClass().getClassLoader().getResource("hotrod-client.properties").toURI();

// Retrieve the system wide Cache Manager via org.infinispan.jcache.remote.JCachingProvider
CacheManager cacheManager = Caching.getCachingProvider("org.infinispan.jcache.remote.JCachingProvider")
      .getCacheManager(uri, this.getClass().getClassLoader(), null);
