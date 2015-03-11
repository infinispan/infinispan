package org.infinispan.tx;


import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.mocks.ControlledCommandFactory;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * test:
 *  - N1 starts a tx with 10 keys that map to the second node and prepares it
 *  - N3 is started and (hopefully) some of the keys touched by the transaction should be migrated over to N3
 *  - the transaction is finalized. The test makes sure that:
 *        -  no data is lost during ST
 *        - the transaction is cleaned up correctly from all nodes
 *
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "functional", testName = "tx.LockCleanupStateTransferTest")
@CleanupAfterMethod
public class LockCleanupStateTransferTest extends MultipleCacheManagersTest {
   private static final int KEY_SET_SIZE = 10;
   private ConfigurationBuilder dcc;

   @Override
   protected void createCacheManagers() throws Throwable {
      dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.transaction().transactionManagerLookup(new DummyTransactionManagerLookup());
      dcc.clustering().hash().numOwners(1);
      dcc.clustering().stateTransfer().fetchInMemoryState(true);
      createCluster(dcc, 2);
      waitForClusterToForm();
   }

   public void testBelatedCommit() throws Throwable {
      testLockReleasedCorrectly(CommitCommand.class);
   }

   public void testBelatedTxCompletionNotificationCommand() throws Throwable {
      testLockReleasedCorrectly(TxCompletionNotificationCommand.class);
   }

   private void testLockReleasedCorrectly(Class<? extends  ReplicableCommand> toBlock ) throws Throwable {

      final ControlledCommandFactory ccf = ControlledCommandFactory.registerControlledCommandFactory(advancedCache(1), toBlock);
      ccf.gate.close();

      final Set<Object> keys = new HashSet<>(KEY_SET_SIZE);

      //fork it into another test as this is going to block in commit
      Future<Object> future = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            tm(0).begin();
            for (int i = 0; i < KEY_SET_SIZE; i++) {
               Object k = getKeyForCache(1);
               keys.add(k);
               cache(0).put(k, k);
            }
            tm(0).commit();
            return null;
         }
      });

      //now wait for all the commits to block
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            log.tracef("receivedCommands == %s", ccf.blockTypeCommandsReceived.get());
            return ccf.blockTypeCommandsReceived.get() == 1;
         }
      });

      if (toBlock == TxCompletionNotificationCommand.class) {
         //at this stage everything should be committed locally
         DataContainer dc = advancedCache(1).getDataContainer();
         for (Object k : keys) {
            assertEquals(k, dc.get(k).getValue());
         }
      }


      log.trace("Before state transfer");

      //now add a one new member
      addClusterEnabledCacheManager(dcc);
      waitForClusterToForm();
      log.trace("After state transfer");

      final Set<Object> migratedKeys = new HashSet<>(KEY_SET_SIZE);
      for (Object key : keys) {
         if (keyMapsToNode(key, 2)) {
            migratedKeys.add(key);
         }
      }

      log.tracef("Number of migrated keys is %s", migratedKeys.size());
      if (migratedKeys.size() == 0) return;

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            int remoteTxCount = TestingUtil.getTransactionTable(cache(2)).getRemoteTxCount();
            log.tracef("remoteTxCount=%s", remoteTxCount);
            return remoteTxCount == 1;
         }
      });

      log.trace("Releasing the gate");
      ccf.gate.open();

      // wait for the forked thread to finish its transaction
      future.get(10, TimeUnit.SECONDS);

      for (int i = 0; i < 3; i++) {
         TransactionTable tt = TestingUtil.getTransactionTable(cache(i));
         assertEquals("For cache " + i, 0, tt.getLocalTxCount());
      }

      // the tx completion is async, so we need to wait a little more
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            boolean success = true;
            for (int i = 0; i < 3; i++) {
               TransactionTable tt = TestingUtil.getTransactionTable(cache(i));
               int remoteTxCount = tt.getRemoteTxCount();
               log.tracef("For cache %s, remoteTxCount==%d", cache(i), remoteTxCount);
               success &= remoteTxCount == 0;
            }
            return success;
         }
      });

      for (Object key : keys) {
         assertNotLocked(key);
         assertEquals(key, cache(0).get(key));
      }

      for (Object k : migratedKeys) {
         assertFalse(advancedCache(0).getDataContainer().containsKey(k));
         assertFalse(advancedCache(1).getDataContainer().containsKey(k));
         assertTrue(advancedCache(2).getDataContainer().containsKey(k));
      }
   }

   private boolean keyMapsToNode(Object key, int nodeIndex) {
      Address owner = owner(key);
      return owner.equals(address(nodeIndex));
   }

   private Address owner(Object key) {
      return advancedCache(0).getDistributionManager().getWriteConsistentHash().locateOwners(key).get(0);
   }

}
