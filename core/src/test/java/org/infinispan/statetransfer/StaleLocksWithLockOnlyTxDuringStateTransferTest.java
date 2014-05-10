package org.infinispan.statetransfer;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.infinispan.AdvancedCache;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.TransactionTable;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

import static org.infinispan.test.concurrent.StateSequencerUtil.*;
import static org.testng.AssertJUnit.assertEquals;

@Test(testName = "lock.StaleLocksWithLockOnlyTxDuringStateTransferTest", groups = "functional")
@CleanupAfterMethod
public class StaleLocksWithLockOnlyTxDuringStateTransferTest extends MultipleCacheManagersTest {
   public static final String CACHE_NAME = "testCache";

   @Override
   protected void createCacheManagers() throws Throwable {
      addClusterEnabledCacheManager();
      addClusterEnabledCacheManager();
      waitForClusterToForm();
   }

   public void testSync() throws Throwable {
      doTest(CacheMode.DIST_SYNC);
   }

   public void testAsync() throws Throwable {
      doTest(CacheMode.DIST_ASYNC);
   }

   private void doTest(CacheMode cacheMode) throws Throwable {
      final StateSequencer sequencer = new StateSequencer();
      sequencer.logicalThread("st", "st:block_get_transactions", "st:resume_get_transactions", "st:block_ch_update", "st:resume_ch_update");
      sequencer.logicalThread("tx", "tx:before_lock", "tx:block_remote_lock", "tx:resume_remote_lock", "tx:after_commit");

      // The lock will be acquired after rebalance has started, but before cache0 starts sending the transaction data to cache1
      sequencer.order("st:block_get_transactions", "tx:before_lock", "tx:block_remote_lock", "st:resume_get_transactions");
      // The tx will be committed (1PC) after cache1 has received all the state, but before the topology is updated
      sequencer.order("st:block_ch_update", "tx:resume_remote_lock", "tx:after_commit", "st:resume_ch_update");

      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.clustering().cacheMode(cacheMode)
            .stateTransfer().awaitInitialTransfer(false)
            .transaction().lockingMode(LockingMode.PESSIMISTIC);
      manager(0).defineConfiguration(CACHE_NAME, cfg.build());
      manager(1).defineConfiguration(CACHE_NAME, cfg.build());

      AdvancedCache<Object, Object> cache0 = advancedCache(0, CACHE_NAME);
      TransactionManager tm0 = cache0.getTransactionManager();
      StateTransferManager stm0 = TestingUtil.extractComponent(cache0, StateTransferManager.class);

      int initialTopologyId = stm0.getCacheTopology().getTopologyId();
      int rebalanceTopologyId = initialTopologyId + 1;
      final int finalTopologyId = rebalanceTopologyId + 1;

      // Block state request commands on cache0 until the lock command has been sent to cache1
      advanceOnComponentMethod(sequencer, cache0, StateProvider.class,
            matchMethodCall("getTransactionsForSegments").build())
            .before("st:block_get_transactions", "st:resume_get_transactions");
      // Block the topology update on cache0 until the tx has finished
      advanceOnGlobalComponentMethod(sequencer, manager(0), LocalTopologyManager.class,
            matchMethodCall("handleConsistentHashUpdate").withMatcher(1, new CacheTopologyMatcher(finalTopologyId)).build())
            .before("st:block_ch_update", "st:resume_ch_update");

      // Start cache 1, but the state request will be blocked on cache 0
      AdvancedCache<Object, Object> cache1 = advancedCache(1, CACHE_NAME);

      // Block the remote lock command on cache 1
      advanceOnInboundRpc(sequencer, manager(1),
            matchCommand(LockControlCommand.class).withCache(CACHE_NAME).build())
            .before("tx:block_remote_lock", "tx:resume_remote_lock");


      // Wait for the rebalance to start
      sequencer.advance("tx:before_lock");
      assertEquals(rebalanceTopologyId, stm0.getCacheTopology().getTopologyId());

      // Start a transaction on cache 0
      MagicKey key = new MagicKey("testkey", cache0);
      tm0.begin();
      cache0.lock(key);
      tm0.commit();

      // Let the rebalance finish
      sequencer.advance("tx:after_commit");

      TestingUtil.waitForRehashToComplete(caches(CACHE_NAME));
      assertEquals(finalTopologyId, stm0.getCacheTopology().getTopologyId());

      // Check for stale locks
      final TransactionTable tt0 = TestingUtil.extractComponent(cache0, TransactionTable.class);
      final TransactionTable tt1 = TestingUtil.extractComponent(cache1, TransactionTable.class);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return tt0.getLocalTxCount() == 0 && tt1.getRemoteTxCount() == 0;
         }
      });

      sequencer.stop();
   }

   private static class CacheTopologyMatcher extends BaseMatcher<Object> {
      private final int topologyId;

      public CacheTopologyMatcher(int topologyId) {
         this.topologyId = topologyId;
      }

      @Override
      public boolean matches(Object item) {
         return (item instanceof CacheTopology) && ((CacheTopology) item).getTopologyId() == topologyId;
      }

      @Override
      public void describeTo(Description description) {
         description.appendText("CacheTopology(" + topologyId + ")");
      }
   }
}
