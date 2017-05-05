package org.infinispan.lock.singlelock;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.HashMap;
import java.util.Map;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.testng.annotations.Test;

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
      dccc.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      dccc.clustering().hash().l1().disable().locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
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
      Map<Object, EmbeddedTransaction> key2Tx = new HashMap<>();
      for (int i = 0; i < NUM_KEYS; i++) {
         Object key = getKeyForCache(0);
         if (key2Tx.containsKey(key)) continue;

         embeddedTm(nodeThatPuts).begin();
         cache(nodeThatPuts).put(key, key);
         EmbeddedTransaction tx = embeddedTm(nodeThatPuts).getTransaction();
         tx.runPrepare();
         embeddedTm(nodeThatPuts).suspend();
         key2Tx.put(key, tx);

         assertLocked(0, key);
      }

      log.trace("Lock transfer happens here");

      addClusterEnabledCacheManager(dccc);
      waitForClusterToForm();

      Object migratedKey = null;
      LocalizedCacheTopology cacheTopology = advancedCache(2).getDistributionManager().getCacheTopology();
      for (Object key : key2Tx.keySet()) {
         if (cacheTopology.getDistribution(key).isPrimary()) {
            migratedKey = key;
            break;
         }
      }
      if (migratedKey == null) {
         log.trace("No key migrated to new owner.");
      } else {
         log.trace("migratedKey = " + migratedKey);
         embeddedTm(2).begin();
         cache(2).put(migratedKey, "someValue");
         try {
            embeddedTm(2).commit();
            fail("RollbackException should have been thrown here.");
         } catch (RollbackException e) {
            //expected
         }
      }

      log.trace("About to commit existing transactions.");

      log.trace("Committing the tx to the new node.");
      for (Transaction tx : key2Tx.values()) {
         tm(nodeThatPuts).resume(tx);
         embeddedTm(nodeThatPuts).getTransaction().runCommit(false);
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
      if (d0 == null || d0.getValue() == null) {
         assert sameValue(d1, d2);
         return d1.getValue();
      } else if (d1 == null || d1.getValue() == null)  {
         assert sameValue(d0, d2);
         return d0.getValue();
      } else  if (d2 == null || d2.getValue() == null) {
         assert sameValue(d0, d1);
         return d0.getValue();
      }
      throw new RuntimeException();
   }

   private boolean sameValue(InternalCacheEntry d1, InternalCacheEntry d2) {
      return d1.getValue().equals(d2.getValue());
   }

   private EmbeddedTransactionManager embeddedTm(int cacheIndex) {
      return (EmbeddedTransactionManager) tm(cacheIndex);
   }
}
