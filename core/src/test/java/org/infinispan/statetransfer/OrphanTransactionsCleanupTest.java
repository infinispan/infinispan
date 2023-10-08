package org.infinispan.statetransfer;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Arrays;

import jakarta.transaction.Transaction;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.ReplicatedControlledConsistentHashFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "statetransfer.OrphanTransactionsCleanupTest")
public class OrphanTransactionsCleanupTest extends MultipleCacheManagersTest {

   protected ConfigurationBuilder configurationBuilder;

   public OrphanTransactionsCleanupTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      configurationBuilder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      // Make the coordinator the primary owner of the only segment
      configurationBuilder.clustering().hash().numSegments(1).consistentHashFactory(new ReplicatedControlledConsistentHashFactory(0));
      configurationBuilder.clustering().stateTransfer().awaitInitialTransfer(false);

      createCluster(ReplicatedControlledConsistentHashFactory.SCI.INSTANCE, configurationBuilder, 2);
      waitForClusterToForm();
   }

   public void testJoinerTransactionSurvives() throws Exception {
      Cache<Object, Object> c0 = manager(0).getCache();
      Cache<Object, Object> c1 = manager(1).getCache();
      final TransactionTable tt0 = TestingUtil.extractComponent(c0, TransactionTable.class);

      // Disable rebalancing so that the joiner is not included in the CH
      LocalTopologyManager ltm0 = TestingUtil.extractGlobalComponent(manager(0), LocalTopologyManager.class);
      ltm0.setRebalancingEnabled(false);

      // Add a new node
      addClusterEnabledCacheManager(ReplicatedControlledConsistentHashFactory.SCI.INSTANCE, configurationBuilder);
      Cache<Object, Object> c2 = manager(2).getCache();

      // Start a transaction from c2, but don't commit yet
      tm(2).begin();
      c2.put("key1", "value1");
      assertEquals(1, tt0.getRemoteGlobalTransaction().size());
      Transaction tx2 = tm(2).suspend();

      // Start another transaction from c1, also without committing it
      tm(1).begin();
      c1.put("key2", "value2");
      assertEquals(2, tt0.getRemoteGlobalTransaction().size());
      Transaction tx1 = tm(1).suspend();

      // Kill node 1 to trigger the orphan transaction cleanup
      manager(1).stop();
      TestingUtil.blockUntilViewsReceived(60000, false, c0, c2);
      // Cache 2 should not be in the CH yet
      TestingUtil.waitForNoRebalance(c0);

      assertEquals(Arrays.asList(address(0)), c0.getAdvancedCache().getDistributionManager().getWriteConsistentHash().getMembers());
      eventuallyEquals(1, () -> tt0.getRemoteTransactions().size());

      // Committing the tx on c2 should succeed
      tm(2).resume(tx2);
      tm(2).commit();
   }
}
