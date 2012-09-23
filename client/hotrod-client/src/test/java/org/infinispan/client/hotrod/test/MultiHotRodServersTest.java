package org.infinispan.client.hotrod.test;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.infinispan.test.TestingUtil.blockUntilCacheStatusAchieved;
import static org.infinispan.test.TestingUtil.blockUntilViewReceived;
import static org.testng.AssertJUnit.assertEquals;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.*;

/**
 * Base test class for Hot Rod tests.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public abstract class MultiHotRodServersTest extends MultipleCacheManagersTest {

   protected List<HotRodServer> servers = new ArrayList<HotRodServer>();
   protected List<RemoteCacheManager> clients = new ArrayList<RemoteCacheManager>();

   protected void createHotRodServers(int num, Configuration defaultCfg) {
      // Start Hot Rod servers
      for (int i = 0; i < num; i++) addHotRodServer(defaultCfg);
      // Verify that default caches should be started
      for (int i = 0; i < num; i++) assert manager(i).getCache() != null;
      // Block until views have been received
      blockUntilViewReceived(manager(0).getCache(), num);
      // Verify that caches running
      for (int i = 0; i < num; i++) {
         blockUntilCacheStatusAchieved(
               manager(i).getCache(), ComponentStatus.RUNNING, 10000);
      }

      if (defaultCfg.clustering().cacheMode().isSynchronous()) {
         // Do a put and verify that is present in other nodes
         cache(0).put("k","v");
         for (int i = 0; i < num; i++) assertEquals("v", cache(i).get("k"));
      } else {
         // It must be asynchronous
         for (int i = 1; i < num; i++) replListener(cache(i)).expect(PutKeyValueCommand.class);
         cache(0).put("k","v");
         for (int i = 1; i < num; i++) {
            replListener(cache(i)).waitForRpc();
            assertEquals("v", cache(i).get("k"));
         }
      }

      for (int i = 0; i < num; i++) {
         Properties props = new Properties();
         props.put(SERVER_LIST, String.format("localhost:%d", server(i).getPort()));
         props.put(PING_ON_STARTUP, "false");
         clients.add(new RemoteCacheManager(props));
      }
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
            server.stop();
      } finally {
         // And then the caches and cache managers
         super.destroy();
      }
   }

   private HotRodServer addHotRodServer(Configuration cfg) {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager(LegacyConfigurationAdaptor.adapt(cfg));
      HotRodServer server = TestHelper.startHotRodServer(cm);
      servers.add(server);
      return server;
   }

   protected HotRodServer server(int i) {
      return servers.get(i);
   }

   protected RemoteCacheManager client(int i) {
      return clients.get(i);
   }

}
