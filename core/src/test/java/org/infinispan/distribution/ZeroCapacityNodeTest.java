package org.infinispan.distribution;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.commons.test.Exceptions.expectException;
import static org.infinispan.test.TestingUtil.extractCacheTopology;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
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
import org.infinispan.test.TestingUtil;
import org.infinispan.test.op.TestFunctionalWriteOperation;
import org.infinispan.test.op.TestOperation;
import org.infinispan.test.op.TestWriteOperation;
import org.infinispan.util.concurrent.TimeoutException;
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
   public void testCapacityFactors(CacheMode cacheMode, ConsistentHashFactory<?> consistentHashFactory) {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(cacheMode);
      cb.clustering().hash().numSegments(NUM_SEGMENTS).consistentHashFactory(consistentHashFactory);
      cb.clustering().hash().capacityFactor(1f);

      String cacheName = "" + cacheMode + consistentHashFactory;
      createCache(cb, cacheName);

      Cache<Object, Object> cache1 = node1.getCache(cacheName);
      Cache<Object, Object> cache2 = node2.getCache(cacheName);
      Cache<Object, Object> zeroCapacityCache = zeroCapacityNode.getCache(cacheName);

      ConsistentHash ch = extractCacheTopology(cache1).getReadConsistentHash();
      assertEquals(1f, capacityFactor(ch, node1), 0.0);
      assertEquals(1f, capacityFactor(ch, node2), 0.0);
      assertEquals(0f, capacityFactor(ch, zeroCapacityNode), 0.0);

      assertEquals(Collections.emptySet(), ch.getPrimarySegmentsForOwner(zeroCapacityNode.getAddress()));
      assertEquals(Collections.emptySet(), ch.getSegmentsForOwner(zeroCapacityNode.getAddress()));
      cache1.stop();

      ConsistentHash ch2 = extractCacheTopology(cache2).getReadConsistentHash();
      assertEquals(Collections.emptySet(), ch2.getPrimarySegmentsForOwner(zeroCapacityNode.getAddress()));
      assertEquals(Collections.emptySet(), ch2.getSegmentsForOwner(zeroCapacityNode.getAddress()));

      // Test simple put and get
      zeroCapacityCache.put("key", "value");
      assertEquals("value", zeroCapacityCache.get("key"));
   }

   public void testReplicatedWriteOperations() {
      String cacheName = "replConditional";
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.REPL_SYNC);
      createCache(builder, cacheName);

      for (TestOperation op : TestWriteOperation.values()) {
         doTestReplicatedWriteOperation(cacheName, op);
      }
      for (TestFunctionalWriteOperation op : TestFunctionalWriteOperation.values()) {
         doTestReplicatedWriteOperation(cacheName, op);
      }
   }

   private void doTestReplicatedWriteOperation(String cacheName, TestOperation op) {
      log.debugf("Testing %s", op);
      for (Cache<Object, Object> cache : caches(cacheName)) {
         String key = String.format("key-%s-%s", op, address(cache));
         op.insertPreviousValue(cache.getAdvancedCache(), key);

         Object result = op.perform(cache.getAdvancedCache(), key);
         assertEquals(op.getReturnValue(), result);

         cache.clear();
         assertTrue(cache.isEmpty());
      }
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

   public void testZeroCapacityFactorNodeStartsFirst() throws Exception {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.clustering().hash().numSegments(NUM_SEGMENTS);
      cb.clustering().remoteTimeout(100);

      ConfigurationBuilder cbZero = new ConfigurationBuilder();
      cbZero.clustering().cacheMode(CacheMode.DIST_SYNC);
      cbZero.clustering().hash().numSegments(NUM_SEGMENTS);
      cbZero.clustering().hash().capacityFactor(0f);
      cbZero.clustering().remoteTimeout(100);

      String cacheName = "testCache";
      Future<Cache<Object, Object>> zeroCapacityNodeFuture =
            fork(() -> zeroCapacityNode.createCache(cacheName, cb.build()));
      Future<Cache<Object, Object>> node1Future =
            fork(() -> node1.createCache(cacheName, cbZero.build()));

      TestingUtil.sleepThread(10);
      assertFalse(zeroCapacityNodeFuture.isDone());
      assertFalse(node1Future.isDone());

      // Node2 is the only one that can create the initial topology
      node2.createCache(cacheName, cb.build());

      node1Future.get(10, SECONDS);
      zeroCapacityNodeFuture.get(10, SECONDS);

      waitForClusterToForm(cacheName);
      ConsistentHash ch3 = consistentHash(0, cacheName);
      assertEquals(0f, capacityFactor(ch3, zeroCapacityNode), 0.0);
      assertEquals(0f, capacityFactor(ch3, node1), 0.0);
      assertEquals(1f, capacityFactor(ch3, node2), 0.0);

      cache(0, cacheName).put("key", "value");
      assertEquals("value", cache(0, cacheName).get("key"));

      // Stop the only non-zero-capacity node
      node2.stop();

      // There is no new cache topology, so any operation will time out
      expectException(TimeoutException.class, () -> cache(0, cacheName).get("key"));
   }

   private ConsistentHash consistentHash(int managerIndex, String cacheName) {
      return cache(managerIndex, cacheName).getAdvancedCache().getDistributionManager().getCacheTopology().getReadConsistentHash();
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
