package org.infinispan.lock.singlelock;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;
import java.util.HashMap;
import java.util.Map;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

/**
 * Main owner changes due to state transfer in a distributed cluster using optimistic locking.
 *
 * @since 5.1
 */
@Test(groups = "functional", testName = "lock.singlelock.MainOwnerChangesLockTest")
@CleanupAfterMethod
public class MainOwnerChangesLockTest extends MultipleCacheManagersTest {

   public static final int NUM_KEYS = 100;
   private ConfigurationBuilder dccc;

   @Override
   protected void createCacheManagers() throws Throwable {
      dccc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true, true);
      dccc.transaction().transactionManagerLookup(new DummyTransactionManagerLookup());
      dccc.clustering().hash().l1().disable().locking().lockAcquisitionTimeout(1000l);
      dccc.clustering().stateTransfer().fetchInMemoryState(true);
      createCluster(dccc, 2);
      waitForClusterToForm();
   }

   public void testLocalTxLockMigration() throws Exception {
      testLockMigration(0);
   }

   public void testRemoteTxLockMigration() throws Exception {
      testLockMigration(1);
   }

   private void testLockMigration(int nodeThatPuts) throws Exception {
      Map<Object, DummyTransaction> key2Tx = new HashMap<Object, DummyTransaction>();
      for (int i = 0; i < NUM_KEYS; i++) {
         Object key = getKeyForCache(0);
         if (key2Tx.containsKey(key)) continue;

         dummyTm(nodeThatPuts).begin();
         cache(nodeThatPuts).put(key, key);
         DummyTransaction tx = dummyTm(nodeThatPuts).getTransaction();
         tx.runPrepare();
         dummyTm(nodeThatPuts).suspend();
         key2Tx.put(key, tx);

         assertLocked(0, key);
      }

      log.trace("Lock transfer happens here");

      addClusterEnabledCacheManager(dccc);
      waitForClusterToForm();

      Object migratedKey = null;
      ConsistentHash ch = advancedCache(2).getDistributionManager().getConsistentHash();
      for (Object key : key2Tx.keySet()) {
         if (ch.locatePrimaryOwner(key).equals(address(2))) {
            migratedKey = key;
            break;
         }
      }
      if (migratedKey == null) {
         log.trace("No key migrated to new owner.");
      } else {
         log.trace("migratedKey = " + migratedKey);
         dummyTm(2).begin();
         cache(2).put(migratedKey, "someValue");
         try {
            dummyTm(2).commit();
            fail("RollbackException should have been thrown here.");
         } catch (RollbackException e) {
            //expected
         }
      }

      log.trace("About to commit existing transactions.");

      log.trace("Committing the tx to the new node.");
      for (Transaction tx : key2Tx.values()) {
         tm(nodeThatPuts).resume(tx);
         dummyTm(nodeThatPuts).getTransaction().runCommitTx();
      }
      
      for (Object key : key2Tx.keySet()) {
         Object value = getValue(key);//make sure that data from the container, just to make sure all replicas are correctly set
         assertEquals(key, value);
      }
   }

   private Object getValue(Object key) {
      log.tracef("Checking key: %s", key);
      InternalCacheEntry d0 = advancedCache(0).getDataContainer().get(key);
      InternalCacheEntry d1 = advancedCache(1).getDataContainer().get(key);
      InternalCacheEntry d2 = advancedCache(2).getDataContainer().get(key);
      if (d0 == null) {
         assert sameValue(d1, d2);
         return d1.getValue();
      } else if (d1 == null)  {
         assert sameValue(d0, d2);
         return d0.getValue();
      } else  if (d2 == null) {
         assert sameValue(d0, d1);
         return d0.getValue();
      }
      throw new RuntimeException();
   }

   private boolean sameValue(InternalCacheEntry d1, InternalCacheEntry d2) {
      return d1.getValue().equals(d2.getValue());
   }

   private DummyTransactionManager dummyTm(int cacheIndex) {
      return (DummyTransactionManager) tm(cacheIndex);
   }
}
