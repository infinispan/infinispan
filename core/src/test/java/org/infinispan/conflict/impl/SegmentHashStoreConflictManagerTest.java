package org.infinispan.conflict.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.conflict.ConflictManager;
import org.infinispan.conflict.ConflictManagerFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.MagicKey;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "conflict.impl.SegmentHashStoreConflictManagerTest")
public class SegmentHashStoreConflictManagerTest extends BasePartitionHandlingTest {

   private static final String CACHE_NAME = "segment-hash-store-conflict-cache";
   private static final int NUMBER_OF_CACHE_ENTRIES = 50;

   public SegmentHashStoreConflictManagerTest() {
      this.cacheMode = CacheMode.DIST_SYNC;
      this.partitionHandling = PartitionHandling.ALLOW_READ_WRITES;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().partitionHandling().whenSplit(partitionHandling).mergePolicy(null)
            .stateTransfer().fetchInMemoryState(true);
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      defineConfigurationOnAllManagers(CACHE_NAME, builder);
   }

   public void testNoConflictsWithStore() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      IntStream.range(0, NUMBER_OF_CACHE_ENTRIES).forEach(i -> getCache(0).put(i, "v" + i));

      assertEquals(0, getConflicts(0).count(), "Expected no conflicts with store-backed cache");
   }

   public void testConflictsDetectedWithStore() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      AdvancedCache<Object, Object> cache = getCache(0);
      IntStream.range(0, NUMBER_OF_CACHE_ENTRIES).forEach(i -> cache.put(i, "v" + i));

      int conflictsIntroduced = 0;
      LocalizedCacheTopology topology = cache.getDistributionManager().getCacheTopology();
      for (int i = 0; i < NUMBER_OF_CACHE_ENTRIES; i += 10) {
         Address primary = topology.getDistribution(i).primary();
         AdvancedCache<Object, Object> primaryCache = manager(primary).getCache(CACHE_NAME)
               .getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL);
         primaryCache.put(i, "CONFLICT");
         conflictsIntroduced++;
      }

      List<Map<Address, CacheEntry<Object, Object>>> conflicts = getConflicts(0).collect(Collectors.toList());
      assertEquals(conflictsIntroduced, conflicts.size(), "All conflicts should be detected with store");
   }

   public void testUpdateWithStoreBackedOldValue() {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      AdvancedCache<Object, Object> cache = getCache(0);
      ConflictManager<Object, Object> cm = ConflictManagerFactory.get(cache);

      MagicKey key = new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME));
      cache.put(key, "original");

      // Update the value — the interceptor should load the old value from the store
      // if it's not available in the MVCC entry
      cache.put(key, "updated");

      assertEquals(0, getConflicts(0).count(), "No conflicts expected after normal update with store");

      // Now introduce a local conflict
      cache.withFlags(Flag.CACHE_MODE_LOCAL).put(key, "conflict");

      long conflictCount = getConflicts(0).count();
      assertTrue(conflictCount >= 1, "Expected at least one conflict after local-only write");
   }

   private AdvancedCache<Object, Object> getCache(int index) {
      return advancedCache(index, CACHE_NAME);
   }

   private Stream<Map<Address, CacheEntry<Object, Object>>> getConflicts(int index) {
      return ConflictManagerFactory.get(getCache(index)).getConflicts();
   }
}
