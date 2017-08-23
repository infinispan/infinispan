package org.infinispan.lock.singlelock.replicated.pessimistic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.lock.singlelock.AbstractNoCrashTest;
import org.infinispan.test.eventually.Eventually;
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

      assert lockManager(0).isLocked(k);
      assert !lockManager(1).isLocked(k);
      assert !lockManager(2).isLocked(k);

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

      assert lockManager(0).isLocked(k1);
      assert lockManager(0).isLocked(k2);
      assert !lockManager(1).isLocked(k1);
      assert !lockManager(1).isLocked(k2);
      assert !lockManager(1).isLocked(k2);
      assert !lockManager(2).isLocked(k2);

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

      assert lockManager(0).isLocked(k0);
      assert !lockManager(1).isLocked(k0);
      assert !lockManager(2).isLocked(k0);

      tm(0).commit();

      assertNotLocked(k0);
      assertValue(k0, false);
   }

   public void testSecondTxCannotPrepare1() throws Exception {
      Object k0 = new MagicKey("k0", cache(0));
      tm(0).begin();
      cache(0).put(k0, "v");
      EmbeddedTransaction dtm = (EmbeddedTransaction) tm(0).suspend();

      assert checkTxCount(0, 1, 0);
      assert checkTxCount(1, 0, 0);
      assert checkTxCount(2, 0, 0);

      tm(0).begin();
      try {
         cache(0).put(k0, "other");
         assert false;
      } catch (Throwable e) {
         tm(0).rollback();
      }

      Eventually.eventually(() -> checkTxCount(0, 1, 0) && checkTxCount(1, 0, 0) && checkTxCount(2, 0, 0));


      tm(1).begin();
      try {
         cache(1).put(k0, "other");
         assert false;
      } catch (Throwable e) {
         tm(0).rollback();
      }

      Eventually.eventually(() -> checkTxCount(0, 1, 0) && checkTxCount(1, 0, 0) && checkTxCount(2, 0, 0));


      tm(0).resume(dtm);
      tm(0).commit();

      assertValue(k0, false);

      Eventually.eventually(() -> noPendingTransactions(0) && noPendingTransactions(1) && noPendingTransactions(2));
   }

   public void testSecondTxCannotPrepare2() throws Exception {
      Object k0 = new MagicKey("k0", cache(0));
      tm(1).begin();
      cache(1).put(k0, "v");
      EmbeddedTransaction dtm = (EmbeddedTransaction) tm(1).suspend();

      assert checkTxCount(0, 0, 1);
      assert checkTxCount(1, 1, 0);
      assert checkTxCount(2, 0, 1);

      tm(0).begin();
      try {
         cache(0).put(k0, "other");
         assert false;
      } catch (Throwable e) {
         tm(0).rollback();
      }

      Eventually.eventually(() -> checkTxCount(0, 0, 1) && checkTxCount(1, 1, 0) && checkTxCount(2, 0, 1));


      tm(1).begin();
      try {
         cache(1).put(k0, "other");
         assert false;
      } catch (Throwable e) {
         tm(0).rollback();
      }

      Eventually.eventually(() -> checkTxCount(0, 0, 1) && checkTxCount(1, 1, 0) && checkTxCount(2, 0, 1));


      tm(0).resume(dtm);
      tm(0).commit();

      assertValue(k0, false);

      Eventually.eventually(() -> noPendingTransactions(0) && noPendingTransactions(1) && noPendingTransactions(2));
   }
}
