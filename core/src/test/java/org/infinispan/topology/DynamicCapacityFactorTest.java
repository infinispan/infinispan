package org.infinispan.topology;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.test.TestingUtil.extractCacheTopology;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.TestingUtil.join;
import static org.infinispan.test.TestingUtil.waitForNoRebalance;
import static org.infinispan.util.BlockingLocalTopologyManager.replaceTopologyManagerDefaultCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.HashConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.BlockingLocalTopologyManager;
import org.testng.annotations.Test;

@CleanupAfterMethod
@Test(groups = "functional", testName = "topology.DynamicCapacityFactorTest")
public class DynamicCapacityFactorTest extends MultipleCacheManagersTest {

   private static final int NUM_SEGMENTS = 60;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC)
        .hash().numSegments(NUM_SEGMENTS).numOwners(2);
      createCluster(cb, 3);
      waitForClusterToForm();
   }

   private static void setCapacityFactor(Cache<?, ?> cache, float value) {
      cache.getCacheConfiguration().clustering().hash()
            .attributes().attribute(HashConfiguration.CAPACITY_FACTOR).set(value);
   }

   private void awaitCapacityFactor(Cache<?, ?> cache, int nodeIndex, float expected) {
      eventually(() -> {
         ConsistentHash ch = extractCacheTopology(cache).getReadConsistentHash();
         Float actual = ch.getCapacityFactors().get(address(nodeIndex));
         return actual != null && Float.compare(actual, expected) == 0;
      });
   }

   public void testReduceCapacityFactorToZero() {
      cache(0).put("key1", "value1");
      cache(0).put("key2", "value2");

      setCapacityFactor(cache(2), 0f);
      awaitCapacityFactor(cache(0), 2, 0f);
      waitForNoRebalance(caches());

      ConsistentHash ch = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(ch.getCapacityFactors().get(address(2))).isEqualTo(0f);
      assertThat(ch.getSegmentsForOwner(address(2))).isEmpty();

      assertThat(cache(2).get("key1")).isEqualTo("value1");
      assertThat(cache(2).get("key2")).isEqualTo("value2");
   }

   public void testIncreaseCapacityFromZero() {
      setCapacityFactor(cache(2), 0f);
      awaitCapacityFactor(cache(0), 2, 0f);
      waitForNoRebalance(caches());

      ConsistentHash chBefore = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(chBefore.getSegmentsForOwner(address(2))).isEmpty();

      setCapacityFactor(cache(2), 1.0f);
      awaitCapacityFactor(cache(0), 2, 1.0f);
      waitForNoRebalance(caches());

      ConsistentHash chAfter = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(chAfter.getCapacityFactors().get(address(2))).isEqualTo(1.0f);
      assertThat(chAfter.getSegmentsForOwner(address(2))).isNotEmpty();
   }

   public void testGradualDrain() {
      cache(0).put("key1", "value1");

      setCapacityFactor(cache(1), 0.5f);
      awaitCapacityFactor(cache(0), 1, 0.5f);
      waitForNoRebalance(caches());

      ConsistentHash chHalf = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(chHalf.getCapacityFactors().get(address(1))).isEqualTo(0.5f);
      int segmentsAtHalf = chHalf.getSegmentsForOwner(address(1)).size();

      setCapacityFactor(cache(1), 0f);
      awaitCapacityFactor(cache(0), 1, 0f);
      waitForNoRebalance(caches());

      ConsistentHash chZero = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(chZero.getCapacityFactors().get(address(1))).isEqualTo(0f);
      assertThat(chZero.getSegmentsForOwner(address(1))).isEmpty();
      assertThat(segmentsAtHalf).isGreaterThan(0);
      assertThat(cache(1).get("key1")).isEqualTo("value1");
   }

   public void testGradualWarmUp() {
      cache(0).put("key1", "value1");

      setCapacityFactor(cache(2), 0f);
      awaitCapacityFactor(cache(0), 2, 0f);
      waitForNoRebalance(caches());

      setCapacityFactor(cache(2), 0.5f);
      awaitCapacityFactor(cache(0), 2, 0.5f);
      waitForNoRebalance(caches());

      ConsistentHash chHalf = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(chHalf.getCapacityFactors().get(address(2))).isEqualTo(0.5f);
      int segmentsAtHalf = chHalf.getSegmentsForOwner(address(2)).size();
      assertThat(segmentsAtHalf).isGreaterThan(0);

      setCapacityFactor(cache(2), 1.0f);
      awaitCapacityFactor(cache(0), 2, 1.0f);
      waitForNoRebalance(caches());

      ConsistentHash chFull = extractCacheTopology(cache(0)).getReadConsistentHash();
      int segmentsAtFull = chFull.getSegmentsForOwner(address(2)).size();
      assertThat(segmentsAtFull).isGreaterThanOrEqualTo(segmentsAtHalf);

      assertThat(cache(2).get("key1")).isEqualTo("value1");
   }

   public void testRejectNegativeCapacityFactor() {
      assertThatThrownBy(() -> setCapacityFactor(cache(0), -1f))
            .isInstanceOf(IllegalArgumentException.class);
   }

   public void testRejectZeroCapacityNodeIncrease() {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder()
            .zeroCapacityNode(true);
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC)
        .hash().numSegments(NUM_SEGMENTS).numOwners(2);
      EmbeddedCacheManager zeroCapacityManager = addClusterEnabledCacheManager(gcb, cb);
      waitForClusterToForm();

      Cache<Object, Object> zeroCache = zeroCapacityManager.getCache();
      assertThatThrownBy(() -> setCapacityFactor(zeroCache, 1.0f))
            .isInstanceOf(IllegalStateException.class);
   }

   public void testLocalCacheNoValidator() {
      ConfigurationBuilder localCb = new ConfigurationBuilder();
      localCb.clustering().cacheMode(CacheMode.LOCAL);
      manager(0).defineConfiguration("local-cache", localCb.build());

      Cache<Object, Object> localCache = manager(0).getCache("local-cache");
      setCapacityFactor(localCache, 0.5f);
      assertThat(localCache.getCacheConfiguration().clustering().hash().capacityFactor()).isEqualTo(0.5f);
   }

   public void testNonBinaryForReplicated() {
      ConfigurationBuilder replCb = new ConfigurationBuilder();
      replCb.clustering().cacheMode(CacheMode.REPL_SYNC);
      manager(0).defineConfiguration("repl-cache", replCb.build());
      manager(1).defineConfiguration("repl-cache", replCb.build());
      manager(2).defineConfiguration("repl-cache", replCb.build());
      waitForClusterToForm("repl-cache");

      Cache<Object, Object> replCache = manager(0).getCache("repl-cache");
      assertThatThrownBy(() -> setCapacityFactor(replCache, 0.5f))
            .isInstanceOf(IllegalArgumentException.class);
   }

   public void testNonBinaryForInvalidation() {
      ConfigurationBuilder invCb = new ConfigurationBuilder();
      invCb.clustering().cacheMode(CacheMode.INVALIDATION_SYNC);
      manager(0).defineConfiguration("inv-cache", invCb.build());
      manager(1).defineConfiguration("inv-cache", invCb.build());
      manager(2).defineConfiguration("inv-cache", invCb.build());
      waitForClusterToForm("inv-cache");

      Cache<Object, Object> invCache = manager(0).getCache("inv-cache");
      assertThatThrownBy(() -> setCapacityFactor(invCache, 0.5f))
            .isInstanceOf(IllegalArgumentException.class);
   }

   public void testConcurrentCapacityUpdateToZero() throws Exception {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC)
        .hash().numSegments(NUM_SEGMENTS).numOwners(2);
      for (int i = 0; i < 2; i++) {
         addClusterEnabledCacheManager(cb);
      }
      waitForClusterToForm();

      IntStream.range(0, 100).forEach(i -> cache(0).put("key-" + i, "value-" + i));

      List<Future<Void>> futures = new ArrayList<>();
      for (int i = 2; i < 5; i++) {
         int idx = i;
         futures.add(fork(() -> {
            setCapacityFactor(cache(idx), 0f);
            return null;
         }));
      }
      for (Future<Void> f : futures) {
         f.get(30, java.util.concurrent.TimeUnit.SECONDS);
      }
      for (int i = 2; i < 5; i++) {
         awaitCapacityFactor(cache(0), i, 0f);
      }
      waitForNoRebalance(caches());

      for (int i = 2; i < 5; i++) {
         ConsistentHash ch = extractCacheTopology(cache(0)).getReadConsistentHash();
         assertThat(ch.getSegmentsForOwner(address(i))).isEmpty();
      }
      IntStream.range(0, 100).forEach(i ->
            assertThat(cache(0).get("key-" + i)).isEqualTo("value-" + i));
   }

   public void testRejectAllZeroCapacity() {
      setCapacityFactor(cache(0), 0f);
      awaitCapacityFactor(cache(0), 0, 0f);
      waitForNoRebalance(caches());

      setCapacityFactor(cache(2), 0f);
      awaitCapacityFactor(cache(0), 2, 0f);
      waitForNoRebalance(caches());

      assertThatThrownBy(() -> setCapacityFactor(cache(1), 0f))
            .hasRootCauseInstanceOf(IllegalArgumentException.class);

      ConsistentHash ch = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(ch.getCapacityFactors().get(address(1))).isGreaterThan(0f);
      assertThat(ch.getSegmentsForOwner(address(1))).isNotEmpty();
   }

   public void testCapacityUpdateWithRebalancingDisabled() throws Exception {
      ClusterTopologyManager ctm = extractGlobalComponent(manager(0), ClusterTopologyManager.class);
      join(ctm.setRebalancingEnabled(false));

      ConsistentHash chBefore = extractCacheTopology(cache(0)).getReadConsistentHash();
      int segmentsBefore = chBefore.getSegmentsForOwner(address(2)).size();

      setCapacityFactor(cache(2), 0f);

      ConsistentHash chAfter = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(chAfter.getSegmentsForOwner(address(2)).size()).isEqualTo(segmentsBefore);

      join(ctm.setRebalancingEnabled(true));
      awaitCapacityFactor(cache(0), 2, 0f);
      waitForNoRebalance(caches());

      ConsistentHash chFinal = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(chFinal.getCapacityFactors().get(address(2))).isEqualTo(0f);
      assertThat(chFinal.getSegmentsForOwner(address(2))).isEmpty();
   }

   public void testMultipleUpdatesWhileRebalancingDisabled() throws Exception {
      ClusterTopologyManager ctm = extractGlobalComponent(manager(0), ClusterTopologyManager.class);
      join(ctm.setRebalancingEnabled(false));

      setCapacityFactor(cache(1), 0.5f);
      setCapacityFactor(cache(2), 0f);

      join(ctm.setRebalancingEnabled(true));
      awaitCapacityFactor(cache(0), 1, 0.5f);
      awaitCapacityFactor(cache(0), 2, 0f);
      waitForNoRebalance(caches());

      ConsistentHash ch = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(ch.getCapacityFactors().get(address(1))).isEqualTo(0.5f);
      assertThat(ch.getCapacityFactors().get(address(2))).isEqualTo(0f);
      assertThat(ch.getSegmentsForOwner(address(2))).isEmpty();
   }

   public void testSameValueNoOp() {
      ConsistentHash chBefore = extractCacheTopology(cache(0)).getReadConsistentHash();
      int topologyBefore = extractCacheTopology(cache(0)).getTopologyId();

      setCapacityFactor(cache(0), 1.0f);

      int topologyAfter = extractCacheTopology(cache(0)).getTopologyId();
      assertThat(topologyAfter).isEqualTo(topologyBefore);
      assertThat(chBefore).isEqualTo(extractCacheTopology(cache(0)).getReadConsistentHash());
   }

   public void testConsecutiveRapidUpdates() throws Exception {
      java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(3);
      Future<Void> f1 = fork(() -> {
         barrier.await(10, TimeUnit.SECONDS);
         setCapacityFactor(cache(1), 0.5f);
         return null;
      });
      Future<Void> f2 = fork(() -> {
         barrier.await(10, TimeUnit.SECONDS);
         setCapacityFactor(cache(1), 0f);
         return null;
      });
      barrier.await(10, TimeUnit.SECONDS);
      f1.get(30, TimeUnit.SECONDS);
      f2.get(30, TimeUnit.SECONDS);
      waitForNoRebalance(caches());

      ConsistentHash ch = extractCacheTopology(cache(0)).getReadConsistentHash();
      float finalFactor = ch.getCapacityFactors().get(address(1));
      assertThat(finalFactor).isIn(0f, 0.5f);
      if (Float.compare(finalFactor, 0f) == 0) {
         assertThat(ch.getSegmentsForOwner(address(1))).isEmpty();
      }
   }

   public void testDataAvailableDuringDrain() {
      IntStream.range(0, 100).forEach(i -> cache(0).put("key-" + i, "value-" + i));

      setCapacityFactor(cache(2), 0.5f);
      awaitCapacityFactor(cache(0), 2, 0.5f);
      waitForNoRebalance(caches());
      IntStream.range(0, 100).forEach(i ->
            assertThat(cache(2).get("key-" + i)).isEqualTo("value-" + i));

      setCapacityFactor(cache(2), 0f);
      awaitCapacityFactor(cache(0), 2, 0f);
      waitForNoRebalance(caches());
      IntStream.range(0, 100).forEach(i ->
            assertThat(cache(2).get("key-" + i)).isEqualTo("value-" + i));
   }

   public void testDataAvailableDuringWarmUp() {
      IntStream.range(0, 100).forEach(i -> cache(0).put("key-" + i, "value-" + i));

      setCapacityFactor(cache(2), 0f);
      awaitCapacityFactor(cache(0), 2, 0f);
      waitForNoRebalance(caches());

      setCapacityFactor(cache(2), 0.5f);
      awaitCapacityFactor(cache(0), 2, 0.5f);
      waitForNoRebalance(caches());
      IntStream.range(0, 100).forEach(i ->
            assertThat(cache(0).get("key-" + i)).isEqualTo("value-" + i));

      setCapacityFactor(cache(2), 1.0f);
      awaitCapacityFactor(cache(0), 2, 1.0f);
      waitForNoRebalance(caches());
      IntStream.range(0, 100).forEach(i ->
            assertThat(cache(0).get("key-" + i)).isEqualTo("value-" + i));
   }

   public void testSetCapacityFactorCompletesAfterStateTransfer() throws Exception {
      BlockingLocalTopologyManager tm = replaceTopologyManagerDefaultCache(manager(2));
      try {
         Future<Void> future = fork(() -> {
            setCapacityFactor(cache(2), 0f);
            return null;
         });

         tm.expectTopologyUpdate().unblock();

         assertThat(future.isDone()).isFalse();
         tm.stopBlocking();

         future.get(30, TimeUnit.SECONDS);
         assertThat(future.isDone()).isTrue();

         waitForNoRebalance(caches());
         ConsistentHash ch = extractCacheTopology(cache(0)).getReadConsistentHash();
         assertThat(ch.getCapacityFactors().get(address(2))).isEqualTo(0f);
         assertThat(ch.getPrimarySegmentsForOwner(address(2))).isEmpty();
      } finally {
         tm.stopBlocking();
      }
   }
}
