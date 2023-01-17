package test.org.infinispan.spring.starter.embedded.actuator;

import io.micrometer.core.instrument.binder.cache.CacheMeterBinder;
import io.micrometer.core.instrument.binder.cache.CacheMeterBinderCompatibilityKit;
import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.starter.embedded.actuator.InfinispanCacheMeterBinder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyList;

public class InfinispanCacheMetricBinderTest extends CacheMeterBinderCompatibilityKit<Cache<String, String>> {

   private static EmbeddedCacheManager cacheManager;
   private InfinispanCacheMeterBinder binder;

   @AfterAll
   public static void cleanup() {
      cacheManager.stop();
   }

   @Override
   public Cache<String, String> createCache() {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.metrics().accurateSize(true);
      cacheManager = new DefaultCacheManager(global.build());
      return cacheManager.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).getOrCreateCache("mycache", new ConfigurationBuilder().statistics().enable().build());
   }

   @Override
   public CacheMeterBinder binder() {
      binder = new InfinispanCacheMeterBinder(cache, emptyList());
      return binder;
   }

   @Override
   public void put(String key, String value) {
      cache.put(key, value);
   }

   @Override
   public String get(String key) {
      return cache.get(key);
   }

   @Test
   void dereferencedCacheIsGarbageCollected() {
     // We can't remove cache ref from the manager only by setting the cache property to null
   }
}
