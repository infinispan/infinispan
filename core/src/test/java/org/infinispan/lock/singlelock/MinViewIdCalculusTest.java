package org.infinispan.lock.singlelock;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;


/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.singlelock.MinViewIdCalculusTest")
@CleanupAfterMethod
public class MinViewIdCalculusTest extends MultipleCacheManagersTest {

   private ConfigurationBuilder c;

   @Override
   protected void createCacheManagers() throws Throwable {
      c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      c
         .transaction()
               .lockingMode(LockingMode.PESSIMISTIC)
               .transactionManagerLookup(new DummyTransactionManagerLookup())
         .clustering()
            .hash().numOwners(3);
      createCluster(c, 2);
      waitForClusterToForm();
   }

   public void testMinViewId1() throws Exception {
      final TransactionTable tt0 = TestingUtil.getTransactionTable(cache(0));
      final TransactionTable tt1 = TestingUtil.getTransactionTable(cache(1));

      StateTransferManager stateTransferManager0 = TestingUtil.extractComponent(cache(0), StateTransferManager.class);
      final int topologyId = stateTransferManager0.getCacheTopology().getTopologyId();

      assertEquals(tt0.getMinTopologyId(), topologyId);
      assertEquals(tt1.getMinTopologyId(), topologyId);

      //add a new cache and check that min view is updated
      log.trace("Adding new node ..");
      addClusterEnabledCacheManager(c);
      waitForClusterToForm();
      log.trace("New node added.");

      final int topologyId2 = stateTransferManager0.getCacheTopology().getTopologyId();
      assertTrue(topologyId2 > topologyId);

      final TransactionTable tt2 = TestingUtil.getTransactionTable(cache(2));
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return tt0.getMinTopologyId() == topologyId2
                  && tt1.getMinTopologyId() == topologyId2
                  && tt2.getMinTopologyId() == topologyId2;
         }
      });
   }

   public void testMinViewId2() throws Exception {
      final TransactionTable tt0 = TestingUtil.getTransactionTable(cache(0));
      final TransactionTable tt1 = TestingUtil.getTransactionTable(cache(1));

      StateTransferManager stateTransferManager0 = TestingUtil.extractComponent(cache(0), StateTransferManager.class);
      final int topologyId = stateTransferManager0.getCacheTopology().getTopologyId();

      tm(1).begin();
      cache(1).put(getKeyForCache(0),"v");
      final DummyTransaction t = (DummyTransaction) tm(1).getTransaction();
      t.runPrepare();
      tm(1).suspend();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 1);
         }
      });

      log.trace("Adding new node ..");
      //add a new cache and check that min view is updated
      addClusterEnabledCacheManager(c);
      waitForClusterToForm();
      log.trace("New node added.");

      final int topologyId2 = stateTransferManager0.getCacheTopology().getTopologyId();
      assertTrue(topologyId2 > topologyId);

      assertEquals(tt0.getMinTopologyId(), topologyId);
      assertEquals(tt1.getMinTopologyId(), topologyId);

      tm(1).resume(t);
      t.runCommitTx();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return tt0.getMinTopologyId() == topologyId2 && tt1.getMinTopologyId() == topologyId2;
         }
      });
   }
}
