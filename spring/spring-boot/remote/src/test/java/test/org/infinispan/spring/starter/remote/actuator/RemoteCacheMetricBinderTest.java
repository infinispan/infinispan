package test.org.infinispan.spring.starter.remote.actuator;

import io.micrometer.core.instrument.binder.cache.CacheMeterBinder;
import io.micrometer.core.instrument.binder.cache.CacheMeterBinderCompatibilityKit;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.starter.remote.actuator.RemoteInfinispanCacheMeterBinderProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import static java.util.Collections.emptyList;

public class RemoteCacheMetricBinderTest extends CacheMeterBinderCompatibilityKit {

   @RegisterExtension
   static InfinispanServerExtension infinispanServerExtension = InfinispanServerExtensionBuilder.server();

   private RemoteCache<String, String> cache;

   @AfterEach
   public void cleanCache() {
      cache.clientStatistics().resetStatistics();
   }

   @Override
   public CacheMeterBinder binder() {
      cache = infinispanServerExtension.hotrod().createRemoteCacheManager().getCache("mycache");
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
