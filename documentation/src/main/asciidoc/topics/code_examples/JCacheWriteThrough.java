import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;

CachingProvider cachingProvider = Caching.getCachingProvider();
CacheManager cacheManager = cachingProvider.getCacheManager();

MutableConfiguration<String, String> configuration = new MutableConfiguration<>();
configuration.setTypes(String.class, String.class);
configuration.setWriteThrough(true);
configuration.setCacheWriterFactory(
      FactoryBuilder.factoryOf(MyCacheWriter.class));

Cache<String, String> cache = cacheManager.createCache("writeThroughCache", configuration);
