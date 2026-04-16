package org.infinispan.topology;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.test.TestingUtil.assertNotDone;
import static org.infinispan.test.TestingUtil.extractCacheTopology;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.TestingUtil.join;
import static org.infinispan.test.TestingUtil.waitForNoRebalance;
import static org.infinispan.testing.Exceptions.expectCompletionException;
import static org.infinispan.util.BlockingLocalTopologyManager.replaceTopologyManagerDefaultCache;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.RemoteException;
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

   private void awaitCapacityFactor(Cache<?, ?> cache, int nodeIndex, float expected) {
      eventually(() -> {
         ConsistentHash ch = extractCacheTopology(cache).getReadConsistentHash();
         Float actual = ch.getCapacityFactors().get(address(nodeIndex));
         return actual != null && Float.compare(actual, expected) == 0;
      });
   }

   public void testReduceCapacityFactorToZero() throws Exception {
      cache(0).put("key1", "value1");
      cache(0).put("key2", "value2");

      join(cache(2).getAdvancedCache().setCapacityFactor(0f));
      awaitCapacityFactor(cache(0), 2, 0f);
      waitForNoRebalance(caches());

      ConsistentHash ch = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(ch.getCapacityFactors().get(address(2))).isEqualTo(0f);
      assertThat(ch.getSegmentsForOwner(address(2))).isEmpty();

      assertThat(cache(2).get("key1")).isEqualTo("value1");
      assertThat(cache(2).get("key2")).isEqualTo("value2");
   }

   public void testIncreaseCapacityFromZero() throws Exception {
      join(cache(2).getAdvancedCache().setCapacityFactor(0f));
      awaitCapacityFactor(cache(0), 2, 0f);
      waitForNoRebalance(caches());

      ConsistentHash chBefore = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(chBefore.getSegmentsForOwner(address(2))).isEmpty();

      join(cache(2).getAdvancedCache().setCapacityFactor(1.0f));
      awaitCapacityFactor(cache(0), 2, 1.0f);
      waitForNoRebalance(caches());

      ConsistentHash chAfter = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(chAfter.getCapacityFactors().get(address(2))).isEqualTo(1.0f);
      assertThat(chAfter.getSegmentsForOwner(address(2))).isNotEmpty();
   }

   public void testGradualDrain() throws Exception {
      cache(0).put("key1", "value1");

      join(cache(1).getAdvancedCache().setCapacityFactor(0.5f));
      awaitCapacityFactor(cache(0), 1, 0.5f);
      waitForNoRebalance(caches());

      ConsistentHash chHalf = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(chHalf.getCapacityFactors().get(address(1))).isEqualTo(0.5f);
      int segmentsAtHalf = chHalf.getSegmentsForOwner(address(1)).size();

      join(cache(1).getAdvancedCache().setCapacityFactor(0f));
      awaitCapacityFactor(cache(0), 1, 0f);
      waitForNoRebalance(caches());

      ConsistentHash chZero = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(chZero.getCapacityFactors().get(address(1))).isEqualTo(0f);
      assertThat(chZero.getSegmentsForOwner(address(1))).isEmpty();
      assertThat(segmentsAtHalf).isGreaterThan(0);
      assertThat(cache(1).get("key1")).isEqualTo("value1");
   }

   public void testGradualWarmUp() throws Exception {
      cache(0).put("key1", "value1");

      join(cache(2).getAdvancedCache().setCapacityFactor(0f));
      awaitCapacityFactor(cache(0), 2, 0f);
      waitForNoRebalance(caches());

      join(cache(2).getAdvancedCache().setCapacityFactor(0.5f));
      awaitCapacityFactor(cache(0), 2, 0.5f);
      waitForNoRebalance(caches());

      ConsistentHash chHalf = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(chHalf.getCapacityFactors().get(address(2))).isEqualTo(0.5f);
      int segmentsAtHalf = chHalf.getSegmentsForOwner(address(2)).size();
      assertThat(segmentsAtHalf).isGreaterThan(0);

      join(cache(2).getAdvancedCache().setCapacityFactor(1.0f));
      awaitCapacityFactor(cache(0), 2, 1.0f);
      waitForNoRebalance(caches());

      ConsistentHash chFull = extractCacheTopology(cache(0)).getReadConsistentHash();
      int segmentsAtFull = chFull.getSegmentsForOwner(address(2)).size();
      assertThat(segmentsAtFull).isGreaterThanOrEqualTo(segmentsAtHalf);

      assertThat(cache(2).get("key1")).isEqualTo("value1");
   }

   public void testRejectNegativeCapacityFactor() {
      assertThatThrownBy(() -> cache(0).getAdvancedCache().setCapacityFactor(-1f))
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
      assertThatThrownBy(() -> zeroCache.getAdvancedCache().setCapacityFactor(1.0f))
            .isInstanceOf(IllegalStateException.class);
   }

   public void testRejectLocalCache() {
      ConfigurationBuilder localCb = new ConfigurationBuilder();
      localCb.clustering().cacheMode(CacheMode.LOCAL);
      manager(0).defineConfiguration("local-cache", localCb.build());

      Cache<Object, Object> localCache = manager(0).getCache("local-cache");
      CompletionStage<Void> stage = localCache.getAdvancedCache().setCapacityFactor(0.5f);
      assertThat(stage.toCompletableFuture().isDone()).isTrue();
   }

   public void testNonBinaryForReplicated() throws Exception {
      ConfigurationBuilder replCb = new ConfigurationBuilder();
      replCb.clustering().cacheMode(CacheMode.REPL_SYNC);
      manager(0).defineConfiguration("repl-cache", replCb.build());
      manager(1).defineConfiguration("repl-cache", replCb.build());
      manager(2).defineConfiguration("repl-cache", replCb.build());
      waitForClusterToForm("repl-cache");

      Cache<Object, Object> replCache = manager(0).getCache("repl-cache");
      ConsistentHash chBefore = extractCacheTopology(replCache).getReadConsistentHash();
      float factorBefore = chBefore.getCapacityFactors().get(address(0));

      join(replCache.getAdvancedCache().setCapacityFactor(0.5f));

      ConsistentHash chAfter = extractCacheTopology(replCache).getReadConsistentHash();
      assertThat(chAfter.getCapacityFactors().get(address(0))).isEqualTo(factorBefore);
   }

   public void testNonBinaryForInvalidation() throws Exception {
      ConfigurationBuilder invCb = new ConfigurationBuilder();
      invCb.clustering().cacheMode(CacheMode.INVALIDATION_SYNC);
      manager(0).defineConfiguration("inv-cache", invCb.build());
      manager(1).defineConfiguration("inv-cache", invCb.build());
      manager(2).defineConfiguration("inv-cache", invCb.build());
      waitForClusterToForm("inv-cache");

      Cache<Object, Object> invCache = manager(0).getCache("inv-cache");
      ConsistentHash chBefore = extractCacheTopology(invCache).getReadConsistentHash();
      float factorBefore = chBefore.getCapacityFactors().get(address(0));

      join(invCache.getAdvancedCache().setCapacityFactor(0.5f));

      ConsistentHash chAfter = extractCacheTopology(invCache).getReadConsistentHash();
      assertThat(chAfter.getCapacityFactors().get(address(0))).isEqualTo(factorBefore);
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
         futures.add(fork(() -> join(cache(idx).getAdvancedCache().setCapacityFactor(0f))));
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

   public void testRejectAllZeroCapacity() throws Exception {
      join(cache(0).getAdvancedCache().setCapacityFactor(0f));
      awaitCapacityFactor(cache(0), 0, 0f);
      waitForNoRebalance(caches());

      join(cache(2).getAdvancedCache().setCapacityFactor(0f));
      awaitCapacityFactor(cache(0), 2, 0f);
      waitForNoRebalance(caches());

      CompletionStage<Void> stage = cache(1).getAdvancedCache().setCapacityFactor(0f);
      expectCompletionException(RemoteException.class, IllegalArgumentException.class, stage);

      ConsistentHash ch = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(ch.getCapacityFactors().get(address(1))).isGreaterThan(0f);
      assertThat(ch.getSegmentsForOwner(address(1))).isNotEmpty();
   }

   public void testCapacityUpdateWithRebalancingDisabled() throws Exception {
      ClusterTopologyManager ctm = extractGlobalComponent(manager(0), ClusterTopologyManager.class);
      join(ctm.setRebalancingEnabled(false));

      ConsistentHash chBefore = extractCacheTopology(cache(0)).getReadConsistentHash();
      int segmentsBefore = chBefore.getSegmentsForOwner(address(2)).size();

      join(cache(2).getAdvancedCache().setCapacityFactor(0f));

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

      join(cache(1).getAdvancedCache().setCapacityFactor(0.5f));
      join(cache(2).getAdvancedCache().setCapacityFactor(0f));

      join(ctm.setRebalancingEnabled(true));
      awaitCapacityFactor(cache(0), 1, 0.5f);
      awaitCapacityFactor(cache(0), 2, 0f);
      waitForNoRebalance(caches());

      ConsistentHash ch = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(ch.getCapacityFactors().get(address(1))).isEqualTo(0.5f);
      assertThat(ch.getCapacityFactors().get(address(2))).isEqualTo(0f);
      assertThat(ch.getSegmentsForOwner(address(2))).isEmpty();
   }

   public void testPerNodeUpdateAllCaches() throws Exception {
      ConfigurationBuilder cb2 = new ConfigurationBuilder();
      cb2.clustering().cacheMode(CacheMode.DIST_SYNC)
         .hash().numSegments(NUM_SEGMENTS).numOwners(2);
      manager(0).defineConfiguration("second-cache", cb2.build());
      manager(1).defineConfiguration("second-cache", cb2.build());
      manager(2).defineConfiguration("second-cache", cb2.build());
      waitForClusterToForm("second-cache");

      join(manager(2).setCapacityFactor(0f));
      awaitCapacityFactor(cache(0), 2, 0f);
      awaitCapacityFactor(manager(0).getCache("second-cache"), 2, 0f);
      waitForNoRebalance(caches());
      waitForNoRebalance(caches("second-cache"));

      ConsistentHash chDefault = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(chDefault.getCapacityFactors().get(address(2))).isEqualTo(0f);
      assertThat(chDefault.getSegmentsForOwner(address(2))).isEmpty();

      ConsistentHash chSecond = extractCacheTopology(manager(0).getCache("second-cache")).getReadConsistentHash();
      assertThat(chSecond.getCapacityFactors().get(address(2))).isEqualTo(0f);
      assertThat(chSecond.getSegmentsForOwner(address(2))).isEmpty();
   }

   public void testPerNodeSkipsInternalCaches() throws Exception {
      InternalCacheRegistry icr = extractGlobalComponent(manager(0), InternalCacheRegistry.class);
      ConfigurationBuilder internalCb = new ConfigurationBuilder();
      internalCb.clustering().cacheMode(CacheMode.REPL_SYNC);
      icr.registerInternalCache("__internal_user", internalCb.build(),
            EnumSet.of(InternalCacheRegistry.Flag.USER, InternalCacheRegistry.Flag.PROTECTED));
      manager(0).getCache("__internal_user");

      join(manager(0).setCapacityFactor(0f));
      awaitCapacityFactor(cache(0), 0, 0f);
      waitForNoRebalance(caches());

      ConsistentHash ch = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(ch.getCapacityFactors().get(address(0))).isEqualTo(0f);
      assertThat(ch.getSegmentsForOwner(address(0))).isEmpty();
   }

   public void testPerNodeMixedCacheModes() throws Exception {
      ConfigurationBuilder replCb = new ConfigurationBuilder();
      replCb.clustering().cacheMode(CacheMode.REPL_SYNC);
      manager(0).defineConfiguration("repl-cache", replCb.build());
      manager(1).defineConfiguration("repl-cache", replCb.build());
      manager(2).defineConfiguration("repl-cache", replCb.build());
      waitForClusterToForm("repl-cache");

      join(manager(0).setCapacityFactor(0.5f));
      awaitCapacityFactor(cache(0), 0, 0.5f);
      waitForNoRebalance(caches());

      ConsistentHash chDist = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(chDist.getCapacityFactors().get(address(0))).isEqualTo(0.5f);

      ConsistentHash chRepl = extractCacheTopology(manager(0).getCache("repl-cache")).getReadConsistentHash();
      assertThat(chRepl.getCapacityFactors().get(address(0))).isEqualTo(1.0f);
   }

   public void testPerNodeZeroAppliesToAll() throws Exception {
      ConfigurationBuilder replCb = new ConfigurationBuilder();
      replCb.clustering().cacheMode(CacheMode.REPL_SYNC);
      manager(0).defineConfiguration("repl-cache", replCb.build());
      manager(1).defineConfiguration("repl-cache", replCb.build());
      manager(2).defineConfiguration("repl-cache", replCb.build());
      waitForClusterToForm("repl-cache");

      join(manager(2).setCapacityFactor(0f));
      awaitCapacityFactor(cache(0), 2, 0f);
      awaitCapacityFactor(manager(0).getCache("repl-cache"), 2, 0f);
      waitForNoRebalance(caches());
      waitForNoRebalance(caches("repl-cache"));

      ConsistentHash chDist = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(chDist.getCapacityFactors().get(address(2))).isEqualTo(0f);
      assertThat(chDist.getSegmentsForOwner(address(2))).isEmpty();

      ConsistentHash chRepl = extractCacheTopology(manager(0).getCache("repl-cache")).getReadConsistentHash();
      assertThat(chRepl.getCapacityFactors().get(address(2))).isEqualTo(0f);
   }

   public void testSameValueNoOp() throws Exception {
      ConsistentHash chBefore = extractCacheTopology(cache(0)).getReadConsistentHash();
      int topologyBefore = extractCacheTopology(cache(0)).getTopologyId();

      join(cache(0).getAdvancedCache().setCapacityFactor(1.0f));

      int topologyAfter = extractCacheTopology(cache(0)).getTopologyId();
      assertThat(topologyAfter).isEqualTo(topologyBefore);
      assertThat(chBefore).isEqualTo(extractCacheTopology(cache(0)).getReadConsistentHash());
   }

   public void testConsecutiveRapidUpdates() throws Exception {
      CompletionStage<Void> first = cache(1).getAdvancedCache().setCapacityFactor(0.5f);
      CompletionStage<Void> second = first.thenCompose(v -> cache(1).getAdvancedCache().setCapacityFactor(0f));
      join(second);
      awaitCapacityFactor(cache(0), 1, 0f);
      waitForNoRebalance(caches());

      ConsistentHash ch = extractCacheTopology(cache(0)).getReadConsistentHash();
      assertThat(ch.getCapacityFactors().get(address(1))).isEqualTo(0f);
      assertThat(ch.getSegmentsForOwner(address(1))).isEmpty();
   }

   public void testDataAvailableDuringDrain() throws Exception {
      IntStream.range(0, 100).forEach(i -> cache(0).put("key-" + i, "value-" + i));

      join(cache(2).getAdvancedCache().setCapacityFactor(0.5f));
      awaitCapacityFactor(cache(0), 2, 0.5f);
      waitForNoRebalance(caches());
      IntStream.range(0, 100).forEach(i ->
            assertThat(cache(2).get("key-" + i)).isEqualTo("value-" + i));

      join(cache(2).getAdvancedCache().setCapacityFactor(0f));
      awaitCapacityFactor(cache(0), 2, 0f);
      waitForNoRebalance(caches());
      IntStream.range(0, 100).forEach(i ->
            assertThat(cache(2).get("key-" + i)).isEqualTo("value-" + i));
   }

   public void testDataAvailableDuringWarmUp() throws Exception {
      IntStream.range(0, 100).forEach(i -> cache(0).put("key-" + i, "value-" + i));

      join(cache(2).getAdvancedCache().setCapacityFactor(0f));
      awaitCapacityFactor(cache(0), 2, 0f);
      waitForNoRebalance(caches());

      join(cache(2).getAdvancedCache().setCapacityFactor(0.5f));
      awaitCapacityFactor(cache(0), 2, 0.5f);
      waitForNoRebalance(caches());
      IntStream.range(0, 100).forEach(i ->
            assertThat(cache(0).get("key-" + i)).isEqualTo("value-" + i));

      join(cache(2).getAdvancedCache().setCapacityFactor(1.0f));
      awaitCapacityFactor(cache(0), 2, 1.0f);
      waitForNoRebalance(caches());
      IntStream.range(0, 100).forEach(i ->
            assertThat(cache(0).get("key-" + i)).isEqualTo("value-" + i));
   }

   public void testSetCapacityFactorCompletesAfterStateTransfer() throws Exception {
      BlockingLocalTopologyManager tm = replaceTopologyManagerDefaultCache(manager(2));
      try {
         CompletionStage<Void> stage = cache(2).getAdvancedCache().setCapacityFactor(0f);

         assertNotDone(stage);

         tm.stopBlocking();
         tm.expectTopologyUpdate().unblock();

         join(stage);

         waitForNoRebalance(caches());
         ConsistentHash ch = extractCacheTopology(cache(0)).getReadConsistentHash();
         assertThat(ch.getCapacityFactors().get(address(2))).isEqualTo(0f);
         assertThat(ch.getPrimarySegmentsForOwner(address(2))).isEmpty();
      } finally {
         tm.stopBlocking();
      }
   }
}
