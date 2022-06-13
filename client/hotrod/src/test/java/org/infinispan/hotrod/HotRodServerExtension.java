package org.infinispan.hotrod;

import java.lang.reflect.Method;

import org.infinispan.api.Infinispan;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.hotrod.configuration.HotRodConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * @since 14.0
 **/
public class HotRodServerExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback {
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
      if (!method.getName().equals(cacheName)) {
         cacheName = method.getName();
         ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
         builder
               .clustering()
               .cacheMode(CacheMode.DIST_SYNC)
               .transaction().cacheStopTimeout(0L);
         hotRodServer.getCacheManager().createCache(cacheName, builder.build());
      }
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
         hotRodServer = HotRodTestingUtil.startHotRodServer(ecm);
      }
   }

   public void stop() {
      if (hotRodServer != null) {
         EmbeddedCacheManager cacheManager = hotRodServer.getCacheManager();
         hotRodServer.stop();
         cacheManager.stop();
      }
   }

   public String cacheName() {
      return cacheName;
   }

   public Infinispan getClient() {
      HotRodConfigurationBuilder builder = new HotRodConfigurationBuilder();
      builder.addServer().host(hotRodServer.getHost()).port(hotRodServer.getPort());
      return Infinispan.create(builder.build());
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
