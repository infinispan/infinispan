package org.infinispan.all.embedded;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.spi.CachingProvider;

import org.junit.Test;

import static javax.cache.expiry.Duration.ONE_HOUR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class EmbeddedAllJCacheTest {
   @Test
   public void testAllEmbedded() {
      CachingProvider cachingProvider = Caching.getCachingProvider();
      CacheManager cacheManager = cachingProvider.getCacheManager();
      //configure the cache
      MutableConfiguration<String, Integer> config = new MutableConfiguration<String, Integer>();
      config.setStoreByValue(true).setTypes(String.class, Integer.class)
            .setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(ONE_HOUR)).setStatisticsEnabled(true);
      //create the cache
      cacheManager.createCache("simpleCache", config);
      //... and then later to get the cache
      Cache<String, Integer> cache = Caching.getCache("simpleCache", String.class, Integer.class);
      //use the cache
      String key = "key";
      Integer value1 = 1;
      cache.put("key", value1);
      Integer value2 = cache.get(key);
      assertEquals(value1, value2);
      cache.remove("key");
      assertNull(cache.get("key"));
      cacheManager.close();
   }
}
