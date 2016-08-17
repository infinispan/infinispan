package org.infinispan.tx.locking;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import javax.transaction.Transaction;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.RemoteException;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional", testName = "tx.locking.PessimisticReplTxTest")
public class PessimisticReplTxTest extends AbstractClusteredTxTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      final ConfigurationBuilder conf = buildConfiguration();
      createCluster(conf, 2);
      waitForClusterToForm();

      k = new MagicKey(cache(0));
   }

   protected ConfigurationBuilder buildConfiguration() {
      final ConfigurationBuilder conf = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      conf.transaction()
            .lockingMode(LockingMode.PESSIMISTIC)
            .transactionManagerLookup(new DummyTransactionManagerLookup())
         .locking().lockAcquisitionTimeout(10L); //fail fast
      return conf;
   }

   public void testTxInProgress1() throws Exception {
      log.info("test :: start");
      try {
         test(0, 0);
      } finally {
         log.info("test :: end");
      }
   }

   public void testTxInProgress2() throws Exception {
      test(0, 1);
   }

   public void testTxInProgress3() throws Exception {
      test(1, 1);
   }

   public void testTxInProgress4() throws Exception {
      test(1, 0);
   }

   public void testLockingInterfaceOnPrimaryOwner() throws Exception {
      testLockingWithRollback(0);
   }

   public void testLockingInterfaceOnBackupOwner() throws Exception {
      testLockingWithRollback(1);
   }

   private void testLockingWithRollback(int executeOn) throws Exception {
      tm(executeOn).begin();
      advancedCache(executeOn).lock(k);
      assertLockingOnRollback();
      assertNoTransactions();
      assertNull(cache(0).get(k));
      assertNull(cache(1).get(k));

      tm(executeOn).begin();
      advancedCache(executeOn).lock(k);
      cache(executeOn).put(k, "v");
      assertLockingOnRollback();
      assertNoTransactions();
      assertNull(cache(0).get(k));
      assertNull(cache(1).get(k));
   }

   // check that two transactions can progress in parallel on the same node
   private void test(int firstTxIndex, int secondTxIndex) throws Exception {
      tm(firstTxIndex).begin();
      cache(firstTxIndex).put(k, "v1");
      final Transaction tx1 = tm(firstTxIndex).suspend();

      //another tx working on the same keys
      tm(secondTxIndex).begin();
      try {
         cache(secondTxIndex).put(k, "v2");
         assert false : "Exception expected";
      } catch (TimeoutException e) {
         //expected
         tm(secondTxIndex).suspend();
      } catch (RemoteException e) {
         assert e.getCause() instanceof TimeoutException;
         //expected
         tm(secondTxIndex).suspend();
      }

      tm(firstTxIndex).resume(tx1);
      tm(firstTxIndex).commit();
      assert cache(0).get(k).equals("v1");

      log.info("Before get...");
      assertNotLocked(k);
      assert cache(1).get(k).equals("v1");
      assertNotLocked(k);
   }

   public void simpleTest() throws Exception {
      tm(0).begin();
      cache(0).put(k,"v");
      tm(0).commit();
      assertEquals(cache(0).get(k), "v");
      assertEquals(cache(1).get(k), "v");
   }


   @Override
   protected void assertLocking() {
      assertTrue(lockManager(0).isLocked(k));
      assertFalse(lockManager(1).isLocked(k));
      prepare();
      assertTrue(lockManager(0).isLocked(k));
      assertFalse(lockManager(1).isLocked(k));
      commit();
      assertFalse(lockManager(0).isLocked(k));
      assertFalse(lockManager(1).isLocked(k));
   }

   @Override
   protected void assertLockingNoChanges() {
      assertTrue(lockManager(0).isLocked(k));
      assertFalse(lockManager(1).isLocked(k));
      prepare();
      assertTrue(lockManager(0).isLocked(k));
      assertFalse(lockManager(1).isLocked(k));
      commit();
      assertFalse(lockManager(0).isLocked(k));
      assertFalse(lockManager(1).isLocked(k));
   }

   @Override
   protected void assertLockingOnRollback() {
      assertTrue(lockManager(0).isLocked(k));
      assertFalse(lockManager(1).isLocked(k));
      rollback();
      assertFalse(lockManager(0).isLocked(k));
      assertFalse(lockManager(1).isLocked(k));
   }
}
