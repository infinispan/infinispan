package io.micrometer.core.instrument.binder.cache;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.starter.remote.metrics.RemoteInfinispanCacheMeterBinderProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public class NotAuthorizedRemoteCacheMetricBinderTest {

   @RegisterExtension
   static InfinispanServerExtension infinispanServerExtension = InfinispanServerExtensionBuilder.server();

   private RemoteCache<String, String> cacheAdminConnection;

   private final MeterRegistry registry = new SimpleMeterRegistry();
   private CacheMeterBinder binder;

   @BeforeEach
   void bindToRegistry() {
      this.binder = this.binder();
      this.binder.bindTo(this.registry);
   }

   public CacheMeterBinder binder() {
      org.infinispan.configuration.cache.ConfigurationBuilder cacheConfigBuilder =
            new org.infinispan.configuration.cache.ConfigurationBuilder();
      cacheConfigBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);
      cacheConfigBuilder.security().authorization().roles("admin");

      StringConfiguration stringConfiguration =
            new StringConfiguration(cacheConfigBuilder.build().toStringConfiguration("mycache"));

      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.statistics().enable();
      clientBuilder.clientIntelligence(ClientIntelligence.BASIC);
      clientBuilder.security()
            .authentication()
            .username(TestUser.ADMIN.getUser())
            .password(TestUser.ADMIN.getPassword());

      RemoteCacheManager remoteCacheManagerAdmin =
            infinispanServerExtension.hotrod().withClientConfiguration(clientBuilder)
                                     .createRemoteCacheManager();
      cacheAdminConnection = remoteCacheManagerAdmin.administration().getOrCreateCache("mycache", stringConfiguration);


      clientBuilder = new ConfigurationBuilder();
      clientBuilder.statistics().enable();
      clientBuilder.clientIntelligence(ClientIntelligence.BASIC);
      clientBuilder.security()
            .authentication()
            .username(TestUser.OBSERVER.getUser())
            .password(TestUser.OBSERVER.getPassword());

      RemoteCacheManager remoteCacheManagerObserver =
            infinispanServerExtension.hotrod().withClientConfiguration(clientBuilder)
                  .createRemoteCacheManager();

      // The cache is got with OBSERVER instead of ADMIN
      RemoteCache<String, String> cacheObserverConnection = remoteCacheManagerObserver.getCache("mycache");
      RemoteInfinispanCacheMeterBinderProvider remoteInfinispanCacheMeterBinderProvider =
            new RemoteInfinispanCacheMeterBinderProvider();
      return (CacheMeterBinder) remoteInfinispanCacheMeterBinderProvider
            .getMeterBinder(new SpringCache(cacheObserverConnection), emptyList());
   }

   @Test
   void puts() {
      cacheAdminConnection.put("k", "v");
      assertThat(this.binder.putCount()).isEqualTo(0L);
      assertThat(this.registry.get("cache.puts").tag("cache", "mycache").functionCounter().count()).isEqualTo(0.0);
   }

   @Test
   void gets() {
      cacheAdminConnection.put("k", "v");
      cacheAdminConnection.get("k");
      cacheAdminConnection.get("does.not.exist");
      assertThat(this.binder.hitCount()).isEqualTo(0L);
      assertThat(this.registry.get("cache.gets").tag("result", "hit").tag("cache", "mycache").functionCounter().count()).isEqualTo(0.0);
      if (this.binder.missCount() != null) {
         assertThat(this.binder.missCount()).isIn(new Object[]{0L});
         assertThat(this.registry.get("cache.gets").tag("result", "miss").tag("cache", "mycache").functionCounter().count()).isIn(new Object[]{0.0});
      }
   }
}
