package test.org.infinispan.spring.starter.remote.actuator;

import static java.util.Collections.emptyList;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.starter.remote.actuator.RemoteInfinispanCacheMeterBinderProvider;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.micrometer.core.instrument.binder.cache.CacheMeterBinder;
import io.micrometer.core.instrument.binder.cache.CacheMeterBinderCompatibilityKit;

public class RemoteCacheMetricBinderTest extends CacheMeterBinderCompatibilityKit {

   @RegisterExtension
   static InfinispanServerExtension infinispanServerExtension =
         InfinispanServerExtensionBuilder.server();

   private RemoteCache<String, String> cache;

   @Override
   public CacheMeterBinder binder() {
      org.infinispan.configuration.cache.ConfigurationBuilder serverBuilder =
            new org.infinispan.configuration.cache.ConfigurationBuilder();
      serverBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);
      StringConfiguration stringConfiguration =
            new StringConfiguration(serverBuilder.build().toStringConfiguration("mycache"));

      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.statistics().enable();
      clientBuilder.security()
            .authentication()
            .username(TestUser.ADMIN.getUser())
            .password(TestUser.ADMIN.getPassword());

      RemoteCacheManager remoteCacheManager =
            infinispanServerExtension.hotrod().withClientConfiguration(clientBuilder)
                                     .createRemoteCacheManager();
      cache = remoteCacheManager.administration().getOrCreateCache("mycache", stringConfiguration);
      RemoteInfinispanCacheMeterBinderProvider remoteInfinispanCacheMeterBinderProvider =
            new RemoteInfinispanCacheMeterBinderProvider();
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
