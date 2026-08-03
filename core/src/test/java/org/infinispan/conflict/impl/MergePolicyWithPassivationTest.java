package org.infinispan.conflict.impl;

import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Verifies that conflict resolution correctly detects entries that have been
 * evicted but stored to a store.
 */
@Test(groups = "functional", testName = "conflict.impl.MergePolicyWithPassivationTest")
public class MergePolicyWithPassivationTest extends BaseMergePolicyTest {

   private static final int MAX_COUNT = 10;

   @Override
   public Object[] factory() {
      return new Object[]{
            new MergePolicyWithPassivationTest(DIST_SYNC, 2, "2N", new int[]{0}, new int[]{1}),
      };
   }

   public MergePolicyWithPassivationTest() {
   }

   public MergePolicyWithPassivationTest(CacheMode cacheMode, int owners, String description,
                                         int[] partition1, int[] partition2) {
      super(cacheMode, owners, description, AvailabilityMode.AVAILABLE, partition1, partition2);
      this.mergePolicy = MergePolicy.PREFERRED_ALWAYS;
   }

   @Override
   protected void customizeCacheConfiguration(ConfigurationBuilder dcc) {
      dcc.persistence()
            .passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class);
      dcc.memory().maxCount(MAX_COUNT);
   }

   @Override
   protected void beforeSplit() {
      super.beforeSplit();
      evictConflictKey();
   }

   @Override
   protected void duringSplit(AdvancedCache preferredPartitionCache, AdvancedCache otherCache) throws Exception {
      super.duringSplit(preferredPartitionCache, otherCache);
      evictConflictKey(preferredPartitionCache);
   }

   private void evictConflictKey() {
      for (int i = 0; i < MAX_COUNT + 1; i++) {
         cache(p0.node(0)).put("filler-before-" + i, "value-" + i);
      }
   }

   private void evictConflictKey(AdvancedCache cache) {
      for (int i = 0; i < MAX_COUNT + 1; i++) {
         cache.put("filler-during-" + i, "value-" + i);
      }
   }
}
