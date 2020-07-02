package test.org.infinispan.spring.starter.remote.extension;

import java.util.stream.Stream;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.commons.test.TestResourceTracker;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

/**
 * Junit 5 simple extension for the hotrod server
 *
 * @author Katia Aresti, karesti@redhat.com
 */
public class InfinispanServerExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback, TestTemplateInvocationContextProvider {

   private HotRodServer hotRodServer;
   private RemoteCacheManager hotRodClient;
   private final String host;
   private final int port;
   private final String[] initialCaches;
   private final boolean startBeforeAll;
   private final boolean stopAfterAll;
   private final boolean startBeforeEach;
   private final boolean stopAfterEach;


   public InfinispanServerExtension(String host, int port, String[] initialCaches, boolean startBeforeAll,
                                    boolean stopAfterAll, boolean startBeforeEach, boolean stopAfterEach) {
      this.host = host;
      this.port = port;
      this.initialCaches = initialCaches;
      this.startBeforeAll = startBeforeAll;
      this.stopAfterAll = stopAfterAll;
      this.startBeforeEach = startBeforeEach;
      this.stopAfterEach = stopAfterEach;
   }

   public static final InfinispanServerExtensionBuilder builder() {
      return new InfinispanServerExtensionBuilder();

   }

   @Override
   public boolean supportsTestTemplate(ExtensionContext extensionContext) {
      return true;
   }

   @Override
   public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
         ExtensionContext extensionContext) {
      return null;
   }

   @Override
   public void beforeAll(ExtensionContext context) throws Exception {
      if (startBeforeAll)
         start();
   }

   @Override
   public void afterAll(ExtensionContext context) throws Exception {
      if (stopAfterAll)
         stop();
   }

   @Override
   public void beforeEach(ExtensionContext context) throws Exception {
      if (startBeforeEach)
         start();
   }

   @Override
   public void afterEach(ExtensionContext context) throws Exception {
      if (stopAfterEach)
         stop();
   }

   public static class InfinispanServerExtensionBuilder {
      private String host = "localhost";
      private int port = 11222;
      private String[] cacheNames = new String[0];
      private boolean startBeforeAll = true;
      private boolean stopAfterAll = true;
      private boolean startBeforeEach;
      private boolean stopAfterEach;

      public InfinispanServerExtensionBuilder host(String host) {
         this.host = host;
         return this;
      }

      public InfinispanServerExtensionBuilder port(int port) {
         this.port = port;
         return this;
      }

      public InfinispanServerExtensionBuilder withCaches(String... cacheName) {
         this.cacheNames = cacheName;
         return this;
      }

      public InfinispanServerExtensionBuilder startBeforerEach(boolean stopBeforeEach) {
         this.startBeforeEach = stopBeforeEach;
         return this;
      }

      public InfinispanServerExtensionBuilder stopAfterEach(boolean stopAfterEach) {
         this.stopAfterEach = stopAfterEach;
         return this;
      }

      public InfinispanServerExtensionBuilder startBeforerAll(boolean stopBeforeAll) {
         this.startBeforeAll = stopBeforeAll;
         return this;
      }

      public InfinispanServerExtensionBuilder stopAfterAll(boolean stopAfterAll) {
         this.stopAfterAll = stopAfterAll;
         return this;
      }

      public InfinispanServerExtension build() {
         return new InfinispanServerExtension(host, port, cacheNames, startBeforeAll, stopAfterAll, startBeforeEach, stopAfterEach);
      }
   }

   public RemoteCacheManager hotRodClient() {
      if (hotRodServer != null && hotRodClient == null) {
         org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
         HotRodServerConfiguration serverConfiguration = hotRodServer.getConfiguration();
         builder.addServer().host(serverConfiguration.publicHost())
               .port(serverConfiguration.publicPort());
         builder.statistics().enable();
         hotRodClient = new RemoteCacheManager(builder.build());
      }
      return hotRodClient;
   }

   public void start() {
      if (hotRodServer == null) {
         TestResourceTracker.setThreadTestName("InfinispanServer");
         ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
         EmbeddedCacheManager ecm = TestCacheManagerFactory.createCacheManager(
               new GlobalConfigurationBuilder().nonClusteredDefault().defaultCacheName("default"),
               configurationBuilder);

         for (String cacheName : initialCaches) {
            ecm.createCache(cacheName, new ConfigurationBuilder().build());
         }
         HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
         serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
         hotRodServer = HotRodTestingUtil.startHotRodServer(ecm, host, port, serverBuilder);
      }
   }

   public void stop() {
      if (hotRodServer != null) {
         hotRodServer.stop();
      }
   }

}
