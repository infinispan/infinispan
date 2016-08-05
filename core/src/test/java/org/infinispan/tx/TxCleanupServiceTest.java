package org.infinispan.tx;


import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.mocks.ControlledCommandFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;

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
      dcc.transaction().transactionManagerLookup(new DummyTransactionManagerLookup());
      consistentHashFactory = new ControlledConsistentHashFactory.Default(1);
      dcc.clustering().hash().numOwners(1).numSegments(1).consistentHashFactory(consistentHashFactory);
      createCluster(dcc, 2);
      waitForClusterToForm();
   }

   public void testTransactionStateNotLost() throws Throwable {
      final ControlledCommandFactory ccf = ControlledCommandFactory.registerControlledCommandFactory(cache(1), CommitCommand.class);
      ccf.gate.close();

      final Map<Object, DummyTransaction> keys2Tx = new HashMap<Object, DummyTransaction>(TX_COUNT);

      int viewId = advancedCache(0).getRpcManager().getTransport().getViewId();

      log.tracef("ViewId before %s", viewId);

      //fork it into another thread as this is going to block in commit
      Future<Object> future = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            for (int i = 0; i < TX_COUNT; i++) {
               Object k = getKeyForCache(1);
               tm(0).begin();
               cache(0).put(k, k);
               DummyTransaction transaction = ((DummyTransactionManager) tm(0)).getTransaction();
               keys2Tx.put(k, transaction);
               tm(0).commit();
            }
            return null;
         }
      });

      //now wait for all the commits to block
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return ccf.blockTypeCommandsReceived.get() == TX_COUNT;
         }
      });

      log.tracef("Viewid middle %s", viewId);


      //now add a one new member
      consistentHashFactory.setOwnerIndexes(2);
      addClusterEnabledCacheManager(dcc);
      waitForClusterToForm();


      viewId = advancedCache(0).getRpcManager().getTransport().getViewId();

      log.tracef("Viewid after before %s", viewId);


      final Map<Object, DummyTransaction> migratedTx = new HashMap<Object, DummyTransaction>(TX_COUNT);
      for (Object key : keys2Tx.keySet()) {
         if (keyMapsToNode(key, 2)) {
            migratedTx.put(key, keys2Tx.get(key));
         }
      }

      log.tracef("Number of migrated tx is %s", migratedTx.size());
      assertEquals(TX_COUNT, migratedTx.size());

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return TestingUtil.getTransactionTable(cache(2)).getRemoteTxCount() == migratedTx.size();
         }
      });

      log.trace("Releasing the gate");
      ccf.gate.open();

      future.get(10, TimeUnit.SECONDS);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return TestingUtil.getTransactionTable(cache(2)).getRemoteTxCount() == 0;
         }
      });


      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
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
         }
      });

      for (Object key : keys2Tx.keySet()) {
         assertNotLocked(key);
         assertEquals(key, cache(0).get(key));
      }
   }

   private boolean keyMapsToNode(Object key, int nodeIndex) {
      Address owner = owner(key);
      return owner.equals(address(nodeIndex));
   }

   private Address owner(Object key) {
      return advancedCache(0).getDistributionManager().getConsistentHash().locatePrimaryOwner(key);
   }

}
