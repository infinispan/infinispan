package org.infinispan.conflict.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.conflict.ConflictManagerFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.MagicKey;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import jakarta.transaction.TransactionManager;

@Test(groups = "functional", testName = "conflict.impl.SegmentHashTransactionConflictManagerTest")
public class SegmentHashTransactionConflictManagerTest extends BasePartitionHandlingTest {

   private static final String CACHE_NAME = "segment-hash-tx-conflict-cache";
   private static final int NUMBER_OF_CACHE_ENTRIES = 50;

   public SegmentHashTransactionConflictManagerTest() {
      this.cacheMode = CacheMode.DIST_SYNC;
      this.partitionHandling = PartitionHandling.ALLOW_READ_WRITES;
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new SegmentHashTransactionConflictManagerTest().lockingMode(LockingMode.OPTIMISTIC),
            new SegmentHashTransactionConflictManagerTest().lockingMode(LockingMode.PESSIMISTIC),
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().partitionHandling().whenSplit(partitionHandling).mergePolicy(null)
            .stateTransfer().fetchInMemoryState(true);
      builder.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .lockingMode(lockingMode);
      defineConfigurationOnAllManagers(CACHE_NAME, builder);
   }

   public void testTransactionalPutNoConflicts() throws Exception {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      AdvancedCache<Object, Object> cache = getCache(0);
      TransactionManager tm = cache.getTransactionManager();

      tm.begin();
      IntStream.range(0, NUMBER_OF_CACHE_ENTRIES).forEach(i -> cache.put(i, "v" + i));
      tm.commit();

      assertEquals(0, getConflicts(0).count(), "No conflicts expected after transactional puts");
   }

   public void testTransactionalConflictsDetected() throws Exception {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      AdvancedCache<Object, Object> cache = getCache(0);
      TransactionManager tm = cache.getTransactionManager();

      tm.begin();
      IntStream.range(0, NUMBER_OF_CACHE_ENTRIES).forEach(i -> cache.put(i, "v" + i));
      tm.commit();

      // Introduce conflicts by writing directly to the primary's local cache
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
      assertEquals(conflictsIntroduced, conflicts.size(), "All conflicts should be detected with transactional cache");
   }

   public void testTransactionalUpdateTracksCorrectly() throws Exception {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      AdvancedCache<Object, Object> cache = getCache(0);
      TransactionManager tm = cache.getTransactionManager();

      MagicKey key = new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME));

      tm.begin();
      cache.put(key, "original");
      tm.commit();

      // Update in a separate transaction
      tm.begin();
      cache.put(key, "updated");
      tm.commit();

      assertEquals(0, getConflicts(0).count(),
            "No conflicts expected — hash should track transactional update correctly");
   }

   public void testTransactionalRemoveTracksCorrectly() throws Exception {
      waitForClusterToForm(CACHE_NAME);
      TestingUtil.waitForNoRebalance(caches());

      AdvancedCache<Object, Object> cache = getCache(0);
      TransactionManager tm = cache.getTransactionManager();

      MagicKey key = new MagicKey(cache(0, CACHE_NAME), cache(1, CACHE_NAME));

      tm.begin();
      cache.put(key, "value");
      tm.commit();

      // Remove in a separate transaction
      tm.begin();
      cache.remove(key);
      tm.commit();

      assertEquals(0, getConflicts(0).count(),
            "No conflicts expected — hash should track transactional remove correctly");
   }

   private AdvancedCache<Object, Object> getCache(int index) {
      return advancedCache(index, CACHE_NAME);
   }

   private Stream<Map<Address, CacheEntry<Object, Object>>> getConflicts(int index) {
      return ConflictManagerFactory.get(getCache(index)).getConflicts();
   }
}
