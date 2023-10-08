package org.infinispan.tx;


import static org.testng.AssertJUnit.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.mocks.ControlledCommandFactory;
import org.testng.annotations.Test;

/**
 * Test for https://issues.jboss.org/browse/ISPN-2383
 */
@CleanupAfterMethod
@Test(groups = "functional", testName = "tx.TxCleanupServiceTest")
public class TxCleanupServiceTest extends MultipleCacheManagersTest {
   private static final int TX_COUNT = 1;
   private ConfigurationBuilder dcc;
   private ControlledConsistentHashFactory consistentHashFactory;

   @Override
   protected void createCacheManagers() throws Throwable {
      dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      consistentHashFactory = new ControlledConsistentHashFactory.Default(1);
      dcc.clustering().hash().numOwners(1).numSegments(1).consistentHashFactory(consistentHashFactory);
      createCluster(ControlledConsistentHashFactory.SCI.INSTANCE, dcc, 2);
      waitForClusterToForm();
   }

   public void testTransactionStateNotLost() throws Throwable {
      final ControlledCommandFactory ccf = ControlledCommandFactory.registerControlledCommandFactory(cache(1), VersionedCommitCommand.class);
      ccf.gate.close();

      final Map<Object, EmbeddedTransaction> keys2Tx = new HashMap<>(TX_COUNT);

      int viewId = advancedCache(0).getRpcManager().getTransport().getViewId();

      log.tracef("ViewId before %s", viewId);

      //fork it into another thread as this is going to block in commit
      Future<Object> future = fork(() -> {
            for (int i = 0; i < TX_COUNT; i++) {
               Object k = getKeyForCache(1);
               tm(0).begin();
               cache(0).put(k, k);
               EmbeddedTransaction transaction = ((EmbeddedTransactionManager) tm(0)).getTransaction();
               keys2Tx.put(k, transaction);
               tm(0).commit();
            }
            return null;

      });

      //now wait for all the commits to block
      eventuallyEquals(TX_COUNT, ccf.blockTypeCommandsReceived::get);

      log.tracef("Viewid middle %s", viewId);


      //now add a one new member
      consistentHashFactory.setOwnerIndexes(2);
      addClusterEnabledCacheManager(ControlledConsistentHashFactory.SCI.INSTANCE, dcc);
      waitForClusterToForm();


      viewId = advancedCache(0).getRpcManager().getTransport().getViewId();

      log.tracef("Viewid after before %s", viewId);


      final Map<Object, EmbeddedTransaction> migratedTx = new HashMap<>(TX_COUNT);
      for (Object key : keys2Tx.keySet()) {
         if (keyMapsToNode2(key)) {
            migratedTx.put(key, keys2Tx.get(key));
         }
      }

      log.tracef("Number of migrated tx is %s", migratedTx.size());
      assertEquals(TX_COUNT, migratedTx.size());

      eventuallyEquals(migratedTx.size(), () -> TestingUtil.getTransactionTable(cache(2)).getRemoteTxCount());

      log.trace("Releasing the gate");
      ccf.gate.open();

      future.get(10, TimeUnit.SECONDS);

      eventuallyEquals(0, () -> TestingUtil.getTransactionTable(cache(2)).getRemoteTxCount());


      eventually(() -> {
         boolean allZero = true;
         for (int i = 0; i < 3; i++) {
            TransactionTable tt = TestingUtil.getTransactionTable(cache(i));
//               assertEquals("For cache " + i, 0, tt.getLocalTxCount());
//               assertEquals("For cache " + i, 0, tt.getRemoteTxCount());
            int local = tt.getLocalTxCount();
            int remote = tt.getRemoteTxCount();
            log.tracef("For cache %d, localTxCount=%s, remoteTxCount=%s", i, local, remote);
            log.tracef(String.format("For cache %s , localTxCount=%s, remoteTxCount=%s", i, local, remote));
            allZero = allZero && (local == 0);
            allZero = allZero && (remote == 0);
         }
         return allZero;
      });

      for (Object key : keys2Tx.keySet()) {
         assertNotLocked(key);
         assertEquals(key, cache(0).get(key));
      }
   }

   private boolean keyMapsToNode2(Object key) {
      Address owner = owner(key);
      return owner.equals(address(2));
   }

   private Address owner(Object key) {
      return advancedCache(0).getDistributionManager().getCacheTopology().getDistribution(key).primary();
   }

}
