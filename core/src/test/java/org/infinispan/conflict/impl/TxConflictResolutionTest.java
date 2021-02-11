package org.infinispan.conflict.impl;

import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Check that conflict resolution completes successfully in transactional caches with autocommit disabled.
 *
 * <ol>
 * <li>do a split, and let k -> A, k -> null in the two partitions</li>
 * <li>merge partitions</li>
 * <li>check that all members read A</li>
 * </ol>
 *
 * <p>See ISPN-12725.</p>
 *
 * @author Dan Berindei
 * @since 12.1
 */
@Test(groups = "functional", testName = "conflict.impl.TxConflictResolutionTest")
public class TxConflictResolutionTest extends BaseMergePolicyTest {

   private static final String VAL = "A";
   private boolean autoCommit;

   @Override
   public Object[] factory() {
      return new Object[] {
            new TxConflictResolutionTest().autoCommit(true).lockingMode(LockingMode.PESSIMISTIC),
            new TxConflictResolutionTest().autoCommit(false).lockingMode(LockingMode.PESSIMISTIC),
            new TxConflictResolutionTest().autoCommit(true).lockingMode(LockingMode.OPTIMISTIC),
            new TxConflictResolutionTest().autoCommit(false).lockingMode(LockingMode.OPTIMISTIC),
      };
   }

   public TxConflictResolutionTest() {
      super(DIST_SYNC, null, new int[]{0,1}, new int[]{2,3});
      this.mergePolicy = MergePolicy.PREFERRED_NON_NULL;
      this.valueAfterMerge = VAL;
   }

   TxConflictResolutionTest autoCommit(boolean autoCommit) {
      this.autoCommit = autoCommit;
      return this;
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), new String[]{"autoCommit"});
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), autoCommit);
   }

   @Override
   protected void customizeCacheConfiguration(ConfigurationBuilder dcc) {
      dcc.transaction()
         .transactionMode(TransactionMode.TRANSACTIONAL)
         .lockingMode(lockingMode)
         .autoCommit(autoCommit);
   }

   @Override
   protected void beforeSplit() {
      conflictKey = new MagicKey(cache(p0.node(0)), cache(p1.node(0)));
   }

   @Override
   protected void duringSplit(AdvancedCache preferredPartitionCache, AdvancedCache otherCache) throws Exception {
      try {
         tm(p0.node(0)).begin();
         cache(p0.node(0)).put(conflictKey, VAL);
      } finally {
         tm(p0.node(0)).commit();
      }

      assertCacheGet(conflictKey, VAL, p0.getNodes());
      assertCacheGet(conflictKey, null, p1.getNodes());
   }

   @Override
   protected void performMerge() {
      assertCacheGet(conflictKey, VAL, p0.getNodes());
      assertCacheGet(conflictKey, null, p1.getNodes());

      partition(0).merge(partition(1), false);
      TestingUtil.waitForNoRebalance(caches());
      assertCacheGet(conflictKey, VAL, cacheIndexes());
   }
}
