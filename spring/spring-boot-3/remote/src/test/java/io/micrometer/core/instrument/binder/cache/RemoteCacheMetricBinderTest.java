package io.micrometer.core.instrument.binder.cache;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.starter.remote.actuator.RemoteInfinispanCacheMeterBinder;
import org.infinispan.spring.starter.remote.actuator.RemoteInfinispanCacheMeterBinderProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public class RemoteCacheMetricBinderTest extends CacheMeterBinderCompatibilityKit<RemoteCache<String, String>> {

   @RegisterExtension
   static InfinispanServerExtension infinispanServerExtension = InfinispanServerExtensionBuilder.server();
   private RemoteCacheManager remoteCacheManager;

   @Override
   public CacheMeterBinder<RemoteCache<String, String>> binder() {
      RemoteInfinispanCacheMeterBinderProvider remoteInfinispanCacheMeterBinderProvider =
            new RemoteInfinispanCacheMeterBinderProvider();
      return (RemoteInfinispanCacheMeterBinder<String, String>) remoteInfinispanCacheMeterBinderProvider
            .getMeterBinder(new SpringCache(cache), emptyList());
   }

   @Override
   public RemoteCache<String, String> createCache() {
      org.infinispan.configuration.cache.ConfigurationBuilder cacheConfigBuilder =
            new org.infinispan.configuration.cache.ConfigurationBuilder();
      cacheConfigBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);

      StringConfiguration stringConfiguration =
            new StringConfiguration(cacheConfigBuilder.build().toStringConfiguration("mycache"));

      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.statistics().enable();
      clientBuilder.security()
            .authentication()
            .username(TestUser.ADMIN.getUser())
            .password(TestUser.ADMIN.getPassword());

      remoteCacheManager = infinispanServerExtension.hotrod().withClientConfiguration(clientBuilder)
            .createRemoteCacheManager();
      return remoteCacheManager.administration().getOrCreateCache("mycache", stringConfiguration);
   }

   @Override
   @Test
   void size() {
      // Do nothing
   }

   @Test
   void nearCacheGaugesAreNotPresent() {
      MeterRegistry registry = new SimpleMeterRegistry();
      RemoteInfinispanCacheMeterBinderProvider provider = new RemoteInfinispanCacheMeterBinderProvider();
      RemoteInfinispanCacheMeterBinder<String, String> binder =
            (RemoteInfinispanCacheMeterBinder<String, String>) provider
                  .getMeterBinder(new SpringCache(cache), emptyList());
      binder.bindTo(registry);

      put("k1", "v1");
      get("k1");

      assertThat(registry.find("cache.near.requests").gauge()).isNull();
      assertThat(registry.find("cache.near.invalidations").gauge()).isNull();
      assertThat(registry.find("cache.near.size").gauge()).isNull();
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
      remoteCacheManager.stop();
   }
}
