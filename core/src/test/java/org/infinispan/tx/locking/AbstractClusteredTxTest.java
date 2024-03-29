package org.infinispan.tx.locking;

import static org.testng.Assert.assertNull;

import java.util.Collections;
import java.util.Map;

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;

import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional")
public abstract class AbstractClusteredTxTest extends MultipleCacheManagersTest {

   Object k;

   public void testPut() throws Exception {
      tm(0).begin();
      cache(0).put(k, "v");
      assertLocking();
   }

   public void testRemove() throws Exception {
      tm(0).begin();
      cache(0).remove(k);
      assertLocking();
   }

   public void testReplace() throws Exception {
      tm(0).begin();
      cache(0).replace(k, "v1");
      assertLockingNoChanges();

      // if the key doesn't exist, replace is a no-op, so it shouldn't acquire locks
      cache(0).put(k, "v1");

      tm(0).begin();
      cache(0).replace(k, "v2");
      assertLocking();
   }

   public void testPutAll() throws Exception {
      Map m = Collections.singletonMap(k, "v");
      tm(0).begin();
      cache(0).putAll(m);
      assertLocking();
   }

   public void testRollbackOnPrimaryOwner() throws Exception {
      testRollback(0);
   }

   public void testRollbackOnBackupOwner() throws Exception {
      testRollback(1);
   }

   private void testRollback(int executeOn) throws Exception {
      tm(executeOn).begin();
      cache(executeOn).put(k, "v");
      assertLockingOnRollback();
      assertNoTransactions();
      assertNull(cache(0).get(k));
      assertNull(cache(1).get(k));
   }

   protected void commit() {
      EmbeddedTransactionManager dtm = (EmbeddedTransactionManager) tm(0);
      try {
         dtm.getTransaction().runCommit(false);
      } catch (HeuristicMixedException | HeuristicRollbackException | RollbackException e) {
         throw new RuntimeException(e);
      }
   }

   protected void prepare() {
      EmbeddedTransactionManager dtm = (EmbeddedTransactionManager) tm(0);
      dtm.getTransaction().runPrepare();
   }

   protected void rollback() {
      EmbeddedTransactionManager dtm = (EmbeddedTransactionManager) tm(0);
      try {
         dtm.getTransaction().rollback();
      } catch (SystemException e) {
         throw new RuntimeException(e);
      }
   }

   protected abstract void assertLocking();

   protected abstract void assertLockingNoChanges();

   protected abstract void assertLockingOnRollback();
}
