package org.infinispan.distribution;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHashFactory;
import org.infinispan.distribution.ch.impl.ScatteredConsistentHashFactory;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.distribution.ch.impl.SyncReplicatedConsistentHashFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test the capacity factor for lite instance
 *
 * @author Katia Aresti
 * @since 9.4
 */
@Test(groups = "functional", testName = "distribution.ch.ZeroCapacityNodeTest")
public class ZeroCapacityNodeTest extends MultipleCacheManagersTest {

   public static final int NUM_SEGMENTS = 60;
   private EmbeddedCacheManager node1;
   private EmbeddedCacheManager node2;
   private EmbeddedCacheManager zeroCapacityNode;

   @Override
   protected void createCacheManagers() throws Throwable {
      node1 = addClusterEnabledCacheManager();
      node2 = addClusterEnabledCacheManager();

      GlobalConfigurationBuilder zeroCapacityBuilder =
            GlobalConfigurationBuilder.defaultClusteredBuilder().zeroCapacityNode(true);
      zeroCapacityNode = addClusterEnabledCacheManager(zeroCapacityBuilder, null);
   }

   @DataProvider(name = "cm_chf")
   protected Object[][] consistentHashFactory() {
      return new Object[][]{
            {CacheMode.DIST_SYNC, new DefaultConsistentHashFactory()},
            {CacheMode.DIST_SYNC, new SyncConsistentHashFactory()},
            {CacheMode.REPL_SYNC, new ReplicatedConsistentHashFactory()},
            {CacheMode.REPL_SYNC, new SyncReplicatedConsistentHashFactory()},
            {CacheMode.SCATTERED_SYNC, new ScatteredConsistentHashFactory()},
            };
   }

   @Test(dataProvider = "cm_chf")
   public void testCapacityFactorContainingAZeroCapacityNode(CacheMode cacheMode,
                                                             ConsistentHashFactory<?> consistentHashFactory) {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(cacheMode);
      cb.clustering().hash().numSegments(NUM_SEGMENTS).consistentHashFactory(consistentHashFactory);
      cb.clustering().hash().capacityFactor(1f);

      String cacheName = "" + cacheMode + consistentHashFactory;
      createCache(cb, cacheName);

      ConsistentHash ch =
            cache(0, cacheName).getAdvancedCache().getDistributionManager().getCacheTopology().getReadConsistentHash();
      assertEquals(1f, capacityFactor(ch, node1), 0.0);
      assertEquals(1f, capacityFactor(ch, node2), 0.0);
      assertEquals(0f, capacityFactor(ch, zeroCapacityNode), 0.0);

      assertEquals(Collections.emptySet(), ch.getPrimarySegmentsForOwner(zeroCapacityNode.getAddress()));
      assertEquals(Collections.emptySet(), ch.getSegmentsForOwner(zeroCapacityNode.getAddress()));
      node1.getCache(cacheName).stop();

      ConsistentHash ch2 =
            cache(0, cacheName).getAdvancedCache().getDistributionManager().getCacheTopology().getReadConsistentHash();
      assertEquals(Collections.emptySet(), ch2.getPrimarySegmentsForOwner(zeroCapacityNode.getAddress()));
      assertEquals(Collections.emptySet(), ch2.getSegmentsForOwner(zeroCapacityNode.getAddress()));
   }

   public void testReplicatedClusteredListener() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.REPL_SYNC);
      cb.clustering().hash().numSegments(NUM_SEGMENTS);
      cb.clustering().hash().capacityFactor(1f);

      String cacheName = "replicated_clustered_listener";
      createCache(cb, cacheName);

      ClusteredListener listener = new ClusteredListener();
      zeroCapacityNode.getCache(cacheName).addListener(listener);
      zeroCapacityNode.getCache(cacheName).put("key1", "value1");
      assertEquals(1, listener.events.get());
      node1.getCache(cacheName).put("key2", "value2");
      assertEquals(2, listener.events.get());
   }

   private void createCache(ConfigurationBuilder cb, String cacheName) {
      node1.createCache(cacheName, cb.build());
      node2.createCache(cacheName, cb.build());
      zeroCapacityNode.createCache(cacheName, cb.build());

      waitForClusterToForm(cacheName);
   }

   private Float capacityFactor(ConsistentHash ch, EmbeddedCacheManager node) {
      return ch.getCapacityFactors().get(node.getAddress());
   }

   @Listener(clustered = true)
   private class ClusteredListener {
      AtomicInteger events = new AtomicInteger();

      @CacheEntryCreated
      public void event(Event event) throws Throwable {
         log.tracef("Received event %s", event);
         events.incrementAndGet();
      }
   }

}
