package org.infinispan.statetransfer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.test.TestingUtil.blockUntilViewsReceived;
import static org.infinispan.test.TestingUtil.extractGlobalComponentRegistry;
import static org.infinispan.test.TestingUtil.waitForRehashToComplete;
import static org.testng.AssertJUnit.assertEquals;

import java.io.ByteArrayInputStream;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.JGroupsConfigBuilder;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.test.fwk.TransportFlags;
import org.jgroups.JChannel;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests concurrent startup of cache managers when the channel is started externally
 * and injected with a JGroupsChannelLookup.
 *
 * @author Dan Berindei
 * @since 8.2
 */
@Test(testName = "statetransfer.ConcurrentStartChanelLookupTest", groups = "functional")
@CleanupAfterMethod
public class ConcurrentStartChanelLookupTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      // The test method will create the cache managers
   }

   @DataProvider(name = "startOrder")
   public Object[][] startOrder() {
      return new Object[][]{{0, 1}, {1, 0}};
   }

   @Test(timeOut = 60000, dataProvider = "startOrder")
   public void testConcurrentStart(int eagerManager, int lazyManager) throws Exception {
      TestResourceTracker.testThreadStarted(this);
      String name1 = TestResourceTracker.getNextNodeName();
      String name2 = TestResourceTracker.getNextNodeName();

      // Create and connect both channels beforehand
      // We need both nodes in the view when the coordinator's ClusterTopologyManagerImpl starts
      // in order to reproduce the ISPN-5106 deadlock
      JChannel ch1 = createChannel(name1, 0);
      JChannel ch2 = createChannel(name2, 1);

      // Use a JGroupsChannelLookup to pass the created channels to the transport
      EmbeddedCacheManager cm1 = createCacheManager(name1, ch1);
      EmbeddedCacheManager cm2 = createCacheManager(name2, ch2);

      try {
         assertEquals(ComponentStatus.INSTANTIATED, extractGlobalComponentRegistry(cm1).getStatus());
         assertEquals(ComponentStatus.INSTANTIATED, extractGlobalComponentRegistry(cm2).getStatus());

         log.debugf("Channels created. Starting the caches");
         Future<Object> repl1Future = fork(() -> manager(eagerManager).getCache("repl"));

         // If eagerManager == 0, the coordinator broadcasts a GET_STATUS command.
         // If eagerManager == 1, the non-coordinator sends a POLICY_GET_STATUS command to the coordinator.
         // We want to start the lazyManager without receiving these commands, then the eagerManager should
         // retry and succeed.
         // Bundling and retransmission in NAKACK2 mean we need an extra wait after lazyManager sent its
         // command, so we don't try to wait for a precise amount of time.
         Thread.sleep(500);

         Future<Object> repl2Future = fork(() -> manager(lazyManager).getCache("repl"));

         repl1Future.get(10, SECONDS);
         repl2Future.get(10, SECONDS);

         Cache<String, String> c1r = cm1.getCache("repl");
         Cache<String, String> c2r = cm2.getCache("repl");

         blockUntilViewsReceived(10000, cm1, cm2);
         waitForRehashToComplete(c1r, c2r);

         c1r.put("key", "value");
         assertEquals("value", c2r.get("key"));
      } finally {
         cm1.stop();
         cm2.stop();

         ch1.close();
         ch2.close();
      }
   }

   private EmbeddedCacheManager createCacheManager(String name1, JChannel ch1) {
      GlobalConfigurationBuilder gcb1 = new GlobalConfigurationBuilder();
      gcb1.transport().nodeName(ch1.getName()).distributedSyncTimeout(10, SECONDS);
      gcb1.globalJmxStatistics().allowDuplicateDomains(true);
      CustomChannelLookup.registerChannel(gcb1, ch1, name1, false);

      ConfigurationBuilder replCfg = new ConfigurationBuilder();
      replCfg.clustering().cacheMode(CacheMode.REPL_SYNC);
      replCfg.clustering().stateTransfer().timeout(10, SECONDS);

      EmbeddedCacheManager cm1 = new DefaultCacheManager(gcb1.build(), replCfg.build(), false);
      registerCacheManager(cm1);
      return cm1;
   }

   private JChannel createChannel(String name, int portRange) throws Exception {
      String configString = JGroupsConfigBuilder
            .getJGroupsConfig(ConcurrentStartChanelLookupTest.class.getName(),
                  new TransportFlags().withPortRange(portRange));
      JChannel channel = new JChannel(new ByteArrayInputStream(configString.getBytes()));
      channel.setName(name);
      channel.connect(ConcurrentStartChanelLookupTest.class.getSimpleName());
      log.tracef("Channel %s connected: %s", channel, channel.getViewAsString());
      return channel;
   }
}
