package org.infinispan.client.hotrod;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.function.Consumer;

import org.infinispan.api.Infinispan;
import org.infinispan.api.configuration.Configuration;
import org.infinispan.client.hotrod.impl.HotRodURI;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.testing.TestResourceTracker;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.FixedLengthFrameDecoder;

/**
 * @since 14.0
 **/
public class HotRodServerExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback {
   private RemoteCacheManager rcm;
   private HotRodServer hotRodServer;
   private String cacheName;

   @Override
   public void afterAll(ExtensionContext extensionContext) throws Exception {
      stop();
   }

   @Override
   public void beforeAll(ExtensionContext extensionContext) throws Exception {
      start();
   }

   @Override
   public void beforeEach(ExtensionContext extensionContext) throws Exception {
      Method method = extensionContext.getTestMethod().orElseThrow();
      cacheName = method.getName();
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder
            .clustering()
            .cacheMode(CacheMode.DIST_SYNC)
            .transaction().cacheStopTimeout(0L);
      hotRodServer.getCacheManager().createCache(cacheName, builder.build());
   }

   public void start() {
      if (hotRodServer == null) {
         TestResourceTracker.setThreadTestName("InfinispanServer");
         GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
         gcb.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
         gcb.transport().defaultTransport();
         gcb.defaultCacheName("default");

         EmbeddedCacheManager ecm = TestCacheManagerFactory.createClusteredCacheManager(
               gcb,
               new ConfigurationBuilder(),
               new TransportFlags());
         ecm.administration().createTemplate("test", new ConfigurationBuilder().template(true).build());

         HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
         serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
         hotRodServer = HotRodTestingUtil.startHotRodServer(ecm, serverBuilder);
      }
   }

   public void stop() {
      if (rcm != null) {
         rcm.stop();
      }

      if (hotRodServer != null) {
         EmbeddedCacheManager cacheManager = hotRodServer.getCacheManager();
         hotRodServer.stop();
         cacheManager.stop();
         hotRodServer = null;
      }
   }

   public String cacheName() {
      return cacheName;
   }

   public Infinispan getClient() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      builder.addServer().host(hotRodServer.getHost()).port(hotRodServer.getPort());
      rcm = new RemoteCacheManager(builder.build()) {
         @Override
         protected Consumer<ChannelPipeline> pipelineWrapper() {
            return cp -> cp.addFirst("1frame", new FixedLengthFrameDecoder(1));
         }
      };
      return Infinispan.create(builder.build(), new Infinispan.Factory() {
         @Override
         public Infinispan create(URI uri) {
            try {
               return create(HotRodURI.create(uri).toConfigurationBuilder().build());
            } catch (Throwable t) {
               // Not a Hot Rod URI
               return null;
            }
         }

         @Override
         public Infinispan create(Configuration configuration) {
            assert configuration instanceof org.infinispan.client.hotrod.configuration.Configuration;
            var hrc = (org.infinispan.client.hotrod.configuration.Configuration) configuration;
            return new HotRod(hrc, rcm);
         }
      });
   }

   public static Builder builder() {
      return new Builder();
   }

   public static class Builder {
      public HotRodServerExtension build() {
         return new HotRodServerExtension();
      }
   }
}
