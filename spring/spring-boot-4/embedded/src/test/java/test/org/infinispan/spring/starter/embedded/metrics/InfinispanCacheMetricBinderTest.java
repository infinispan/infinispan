package test.org.infinispan.spring.starter.embedded.metrics;

import static java.util.Collections.emptyList;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.starter.embedded.metrics.InfinispanCacheMeterBinder;
import org.junit.jupiter.api.AfterEach;

import io.micrometer.core.instrument.binder.cache.CacheMeterBinder;
import io.micrometer.core.instrument.binder.cache.CacheMeterBinderCompatibilityKit;

public class InfinispanCacheMetricBinderTest extends CacheMeterBinderCompatibilityKit<Cache<String, String>> {

   private EmbeddedCacheManager cacheManager;

   @AfterEach
   public void cleanup() {
      Util.close(cacheManager);
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
   public CacheMeterBinder<Cache<String, String>> binder() {
      return new InfinispanCacheMeterBinder(cache, emptyList());
   }

   @Override
   public void put(String key, String value) {
      cache.put(key, value);
   }

   @Override
   public String get(String key) {
      return cache.get(key);
   }

   @Override
   public void dereferenceCache() {
      super.dereferenceCache();
      Util.close(cacheManager);
      cacheManager = null;
   }
}
