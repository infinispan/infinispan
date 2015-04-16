package org.infinispan.lock.singlelock.replicated.optimistic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.lock.singlelock.AbstractNoCrashTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.tm.DummyTransaction;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.singlelock.replicated.optimistic.BasicSingleLockReplOptTest")
public class BasicSingleLockReplOptTest extends AbstractNoCrashTest {

   public BasicSingleLockReplOptTest() {
      super(CacheMode.REPL_SYNC, LockingMode.OPTIMISTIC, false);
   }

   protected void testTxAndLockOnDifferentNodes(Operation operation, boolean addFirst, boolean removed) throws Exception {

      if (addFirst)
         cache(0).put("k", "v_initial");

      assertNotLocked("k");

      tm(0).begin();
      operation.perform("k", 0);
      DummyTransaction dtm = (DummyTransaction) tm(0).getTransaction();
      dtm.runPrepare();

      assert lockManager(0).isLocked("k");
      assert !lockManager(1).isLocked("k");
      assert !lockManager(2).isLocked("k");

      dtm.runCommit(false);

      assertNotLocked("k");

      assertValue("k", removed);
   }

   public void testMultipleLocksInSameTx() throws Exception {
      final Object k1 = "k1";
      final Object k2 = "k2";

      tm(0).begin();
      cache(0).put(k1, "v");
      cache(0).put(k2, "v");
      DummyTransaction dtm = (DummyTransaction) tm(0).getTransaction();
      dtm.runPrepare();

      assert lockManager(0).isLocked(k1);
      assert lockManager(0).isLocked(k2);
      assert !lockManager(1).isLocked(k1);
      assert !lockManager(1).isLocked(k2);
      assert !lockManager(1).isLocked(k2);
      assert !lockManager(2).isLocked(k2);

      dtm.runCommit(false);

      assertNotLocked(k1);
      assertNotLocked(k2);
      assertValue(k1, false);
      assertValue(k2, false);
   }

   public void testTxAndLockOnSameNode() throws Exception {

      tm(0).begin();
      cache(0).put("k0", "v");
      DummyTransaction dtm = (DummyTransaction) tm(0).getTransaction();

      dtm.runPrepare();

      assert lockManager(0).isLocked("k0");
      assert !lockManager(1).isLocked("k0");
      assert !lockManager(2).isLocked("k0");

      dtm.runCommit(false);

      assertNotLocked("k0");
      assertValue("k0", false);
   }

   public void testSecondTxCannotPrepare() throws Exception {

      tm(0).begin();
      cache(0).put("k0", "v");
      DummyTransaction dtm = (DummyTransaction) tm(0).getTransaction();
      dtm.runPrepare();
      tm(0).suspend();

      assert checkTxCount(0, 1, 0);
      assert checkTxCount(1, 0, 1);
      assert checkTxCount(2, 0, 1);

      tm(0).begin();
      cache(0).put("k0", "other");
      try {
         tm(0).commit();
         assert false;
      } catch (Throwable e) {
         //ignore
      }

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 1, 0) && checkTxCount(1, 0, 1) && checkTxCount(2, 0, 1);
         }
      });


      tm(1).begin();
      cache(1).put("k0", "other");
      try {
         tm(1).commit();
         assert false;
      } catch (Throwable e) {
         //expected
      }

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 1, 0) && checkTxCount(1, 0, 1) && checkTxCount(2, 0, 1);
         }
      });


      tm(0).resume(dtm);
      dtm.runCommit(false);

      assertValue("k0", false);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return noPendingTransactions(0) && noPendingTransactions(1) && noPendingTransactions(2);
         }
      });
   }
}
