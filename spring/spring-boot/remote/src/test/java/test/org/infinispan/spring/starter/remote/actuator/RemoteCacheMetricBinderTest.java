package test.org.infinispan.spring.starter.remote.actuator;

import static java.util.Collections.emptyList;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.starter.remote.actuator.RemoteInfinispanCacheMeterBinderProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import io.micrometer.core.instrument.binder.cache.CacheMeterBinder;
import io.micrometer.core.instrument.binder.cache.CacheMeterBinderCompatibilityKit;
import test.org.infinispan.spring.starter.remote.extension.InfinispanServerExtension;

public class RemoteCacheMetricBinderTest extends CacheMeterBinderCompatibilityKit {
   private static InfinispanServerExtension infinispanServerExtension = InfinispanServerExtension.builder()
         .withCaches("mycache").build();

   private RemoteCache<String, String> cache;

   @AfterEach
   public void cleanCache() {
      cache.clientStatistics().resetStatistics();
   }

   @BeforeAll
   public static void start() {
      infinispanServerExtension.start();
   }

   @AfterAll
   public static void stop() {
      infinispanServerExtension.stop();
   }

   @Override
   public CacheMeterBinder binder() {
      cache = infinispanServerExtension.hotRodClient().getCache("mycache");
      RemoteInfinispanCacheMeterBinderProvider remoteInfinispanCacheMeterBinderProvider = new RemoteInfinispanCacheMeterBinderProvider();
      return (CacheMeterBinder) remoteInfinispanCacheMeterBinderProvider
            .getMeterBinder(new SpringCache(cache), emptyList());
   }

   @Override
   public void put(String key, String value) {
      cache.put(key, value);
   }

   @Override
   public String get(String key) {
      return cache.get(key);
   }
}
