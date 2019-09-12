package org.infinispan.lock.singlelock;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.testng.annotations.Test;


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
               .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
         .clustering()
            .hash().numOwners(3);
      createCluster(TestDataSCI.INSTANCE, c, 2);
      waitForClusterToForm();
   }

   private void createNewNode() {
      //add a new cache and check that min view is updated
      log.trace("Adding new node ..");
      addClusterEnabledCacheManager(TestDataSCI.INSTANCE, c);
      waitForClusterToForm();
      log.trace("New node added.");
   }

   public void testMinViewId1() throws Exception {
      final TransactionTable tt0 = TestingUtil.getTransactionTable(cache(0));
      final TransactionTable tt1 = TestingUtil.getTransactionTable(cache(1));

      DistributionManager distributionManager0 = advancedCache(0).getDistributionManager();
      final int topologyId = distributionManager0.getCacheTopology().getTopologyId();

      assertEquals(tt0.getMinTopologyId(), topologyId);
      assertEquals(tt1.getMinTopologyId(), topologyId);

      createNewNode();

      final int topologyId2 = distributionManager0.getCacheTopology().getTopologyId();
      assertTrue(topologyId2 > topologyId);

      final TransactionTable tt2 = TestingUtil.getTransactionTable(cache(2));
      eventually(() -> tt0.getMinTopologyId() == topologyId2
            && tt1.getMinTopologyId() == topologyId2
            && tt2.getMinTopologyId() == topologyId2);
   }

   public void testMinViewId2() throws Exception {
      final TransactionTable tt0 = TestingUtil.getTransactionTable(cache(0));
      final TransactionTable tt1 = TestingUtil.getTransactionTable(cache(1));

      DistributionManager distributionManager0 = advancedCache(0).getDistributionManager();
      final int topologyId = distributionManager0.getCacheTopology().getTopologyId();

      tm(1).begin();
      cache(1).put(getKeyForCache(0),"v");
      final EmbeddedTransaction t = (EmbeddedTransaction) tm(1).getTransaction();
      t.runPrepare();
      tm(1).suspend();

      eventually(() -> checkTxCount(0, 0, 1));

      createNewNode();

      final int topologyId2 = distributionManager0.getCacheTopology().getTopologyId();
      assertTrue(topologyId2 > topologyId);

      assertEquals(tt0.getMinTopologyId(), topologyId);
      assertEquals(tt1.getMinTopologyId(), topologyId);

      tm(1).resume(t);
      t.runCommit(false);

      eventually(() -> tt0.getMinTopologyId() == topologyId2 && tt1.getMinTopologyId() == topologyId2);
   }
}
