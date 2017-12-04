package org.infinispan.client.hotrod.test;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.test.TestingUtil.blockUntilCacheStatusAchieved;
import static org.infinispan.test.TestingUtil.blockUntilViewReceived;
import static org.infinispan.test.TestingUtil.killCacheManagers;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;

/**
 * Base test class for Hot Rod tests.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public abstract class MultiHotRodServersTest extends MultipleCacheManagersTest {

   protected final List<HotRodServer> servers = new ArrayList<>();
   protected final List<RemoteCacheManager> clients = new ArrayList<>();

   protected void createHotRodServers(int num, ConfigurationBuilder defaultBuilder) {
      // Start Hot Rod servers
      for (int i = 0; i < num; i++) addHotRodServer(defaultBuilder);
      // Verify that default caches should be started
      for (int i = 0; i < num; i++) assert manager(i).getCache() != null;
      // Block until views have been received
      blockUntilViewReceived(manager(0).getCache(), num);
      // Verify that caches running
      for (int i = 0; i < num; i++) {
         blockUntilCacheStatusAchieved(
               manager(i).getCache(), ComponentStatus.RUNNING, 10000);
      }

      for (int i = 0; i < num; i++) {
         clients.add(createClient(i));
      }
   }

   protected RemoteCacheManager createClient(int i) {
      return new InternalRemoteCacheManager(createHotRodClientConfigurationBuilder(server(i).getPort()).build());
   }

   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer()
            .host("localhost")
            .port(serverPort)
            .maxRetries(maxRetries());
      return clientBuilder;
   }

   protected int maxRetries() {
      return 0;
   }

   @AfterMethod(alwaysRun = true)
   protected void clearContent() throws Throwable {
      // Do not clear content to allow servers
      // to stop gracefully and catch any issues there.
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      // Correct order is to stop servers first
      try {
         for (HotRodServer server : servers)
            HotRodClientTestingUtil.killServers(server);
      } finally {
         // And then the caches and cache managers
         super.destroy();
      }
   }

   protected HotRodServer addHotRodServer(ConfigurationBuilder builder) {
      GlobalConfigurationBuilder globalConfigurationBuilder = getServerModeGlobalConfigurationBuilder();
      modifyGlobalConfiguration(globalConfigurationBuilder);
      EmbeddedCacheManager cm = addClusterEnabledCacheManager(globalConfigurationBuilder, builder);
      HotRodServer server = HotRodClientTestingUtil.startHotRodServer(cm);
      servers.add(server);
      return server;
   }

   private GlobalConfigurationBuilder getServerModeGlobalConfigurationBuilder() {
      GlobalConfigurationBuilder globalConfigurationBuilder = new GlobalConfigurationBuilder();
      globalConfigurationBuilder.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      globalConfigurationBuilder.transport().defaultTransport();
      return globalConfigurationBuilder;
   }

   protected HotRodServer addHotRodServer(ConfigurationBuilder builder, int port) {
      GlobalConfigurationBuilder globalConfigurationBuilder = getServerModeGlobalConfigurationBuilder();

      EmbeddedCacheManager cm = addClusterEnabledCacheManager(globalConfigurationBuilder, builder);
      HotRodServer server = HotRodTestingUtil.startHotRodServer(cm, port, new HotRodServerConfigurationBuilder());
      servers.add(server);
      return server;
   }

   protected HotRodServer server(int i) {
      return servers.get(i);
   }

   protected void killServer(int i) {
      HotRodServer server = servers.get(i);
      killServers(server);
      servers.remove(i);
      killCacheManagers(cacheManagers.get(i));
      cacheManagers.remove(i);
   }

   protected RemoteCacheManager client(int i) {
      return clients.get(i);
   }

   protected void defineInAll(String cacheName, ConfigurationBuilder builder) {
      for (HotRodServer server : servers) {
         server.getCacheManager().defineConfiguration(cacheName, builder.build());
         server.getCacheManager().getCache(cacheName);
      }
   }

   protected void modifyGlobalConfiguration(GlobalConfigurationBuilder builder) {

   }
}
