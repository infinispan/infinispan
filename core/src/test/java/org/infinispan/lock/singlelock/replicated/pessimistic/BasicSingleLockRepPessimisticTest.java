package org.infinispan.lock.singlelock.replicated.pessimistic;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.lock.singlelock.AbstractNoCrashTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.singlelock.replicated.pessimistic.BasicSingleLockRepPessimisticTest")
public class BasicSingleLockRepPessimisticTest extends AbstractNoCrashTest {

   public BasicSingleLockRepPessimisticTest() {
      super(CacheMode.REPL_SYNC, LockingMode.PESSIMISTIC, false);
   }

   protected void testTxAndLockOnDifferentNodes(AbstractNoCrashTest.Operation operation, boolean addFirst, boolean removed) throws Exception {
      Object k = new MagicKey("k", cache(0));
      if (addFirst)
         cache(0).put(k, "v_initial");
      assertNotLocked(k);
      tm(0).begin();
      operation.perform(k, 0);

      assertTrue(lockManager(0).isLocked(k));
      assertFalse(lockManager(1).isLocked(k));
      assertFalse(lockManager(2).isLocked(k));

      tm(0).commit();

      assertNotLocked(k);
      assertValue(k, removed);
   }

   public void testMultipleLocksInSameTx() throws Exception {
      Object k1 = new MagicKey("k1", cache(0));
      Object k2 = new MagicKey("k2", cache(0));

      tm(0).begin();
      cache(0).put(k1, "v");
      cache(0).put(k2, "v");

      assertTrue(lockManager(0).isLocked(k1));
      assertTrue(lockManager(0).isLocked(k2));
      assertFalse(lockManager(1).isLocked(k1));
      assertFalse(lockManager(1).isLocked(k2));
      assertFalse(lockManager(1).isLocked(k2));
      assertFalse(lockManager(2).isLocked(k2));

      tm(0).commit();

      assertNotLocked(k1);
      assertNotLocked(k2);
      assertValue(k1, false);
      assertValue(k2, false);
   }

   public void testTxAndLockOnSameNode() throws Exception {
      Object k0 = new MagicKey("k0", cache(0));
      tm(0).begin();
      cache(0).put(k0, "v");

      assertTrue(lockManager(0).isLocked(k0));
      assertFalse(lockManager(1).isLocked(k0));
      assertFalse(lockManager(2).isLocked(k0));

      tm(0).commit();

      assertNotLocked(k0);
      assertValue(k0, false);
   }

   public void testSecondTxCannotPrepare1() throws Exception {
      Object k0 = new MagicKey("k0", cache(0));
      tm(0).begin();
      cache(0).put(k0, "v");
      EmbeddedTransaction dtm = (EmbeddedTransaction) tm(0).suspend();

      assertTrue(checkTxCount(0, 1, 0));
      assertTrue(checkTxCount(1, 0, 0));
      assertTrue(checkTxCount(2, 0, 0));

      tm(0).begin();
      try {
         cache(0).put(k0, "other");
         fail();
      } catch (Throwable e) {
         tm(0).rollback();
      }

      eventually(() -> checkTxCount(0, 1, 0) && checkTxCount(1, 0, 0) && checkTxCount(2, 0, 0));


      tm(1).begin();
      try {
         cache(1).put(k0, "other");
         fail();
      } catch (Throwable e) {
         tm(0).rollback();
      }

      eventually(() -> checkTxCount(0, 1, 0) && checkTxCount(1, 0, 0) && checkTxCount(2, 0, 0));


      tm(0).resume(dtm);
      tm(0).commit();

      assertValue(k0, false);

      eventually(() -> noPendingTransactions(0) && noPendingTransactions(1) && noPendingTransactions(2));
   }

   public void testSecondTxCannotPrepare2() throws Exception {
      Object k0 = new MagicKey("k0", cache(0));
      tm(1).begin();
      cache(1).put(k0, "v");
      EmbeddedTransaction dtm = (EmbeddedTransaction) tm(1).suspend();

      assertTrue(checkTxCount(0, 0, 1));
      assertTrue(checkTxCount(1, 1, 0));
      assertTrue(checkTxCount(2, 0, 1));

      tm(0).begin();
      try {
         cache(0).put(k0, "other");
         fail();
      } catch (Throwable e) {
         tm(0).rollback();
      }

      eventually(() -> checkTxCount(0, 0, 1) && checkTxCount(1, 1, 0) && checkTxCount(2, 0, 1));


      tm(1).begin();
      try {
         cache(1).put(k0, "other");
         fail();
      } catch (Throwable e) {
         tm(0).rollback();
      }

      eventually(() -> checkTxCount(0, 0, 1) && checkTxCount(1, 1, 0) && checkTxCount(2, 0, 1));


      tm(0).resume(dtm);
      tm(0).commit();

      assertValue(k0, false);

      eventually(() -> noPendingTransactions(0) && noPendingTransactions(1) && noPendingTransactions(2));
   }
}
