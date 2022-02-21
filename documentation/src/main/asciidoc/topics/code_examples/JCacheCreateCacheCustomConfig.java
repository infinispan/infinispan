import java.net.URI;
import javax.cache.*;
import javax.cache.configuration.*;

// Load configuration from an absolute filesystem path
URI uri = URI.create("file:///path/to/infinispan.xml");
// Load configuration from a classpath resource
// URI uri = this.getClass().getClassLoader().getResource("infinispan.xml").toURI();

// Create a Cache Manager using the above configuration
CacheManager cacheManager = Caching.getCachingProvider().getCacheManager(uri, this.getClass().getClassLoader(), null);
