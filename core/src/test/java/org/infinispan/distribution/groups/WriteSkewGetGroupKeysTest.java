package org.infinispan.distribution.groups;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.Map;

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

import org.infinispan.transaction.WriteSkewException;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * It tests the grouping advanced interface for transactional caches with write-skew check enabled.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "distribution.groups.WriteSkewGetGroupKeysTest")
public class WriteSkewGetGroupKeysTest extends TransactionalGetGroupKeysTest {
   @Override
   public Object[] factory() {
      return new Object[]{
            new WriteSkewGetGroupKeysTest(TestCacheFactory.PRIMARY_OWNER),
            new WriteSkewGetGroupKeysTest(TestCacheFactory.BACKUP_OWNER),
            new WriteSkewGetGroupKeysTest(TestCacheFactory.NON_OWNER),
      };
   }

   public WriteSkewGetGroupKeysTest() {
      super(null);
   }

   public WriteSkewGetGroupKeysTest(TestCacheFactory factory) {
      super(factory);
      isolationLevel = IsolationLevel.REPEATABLE_READ;
   }

   public void testRemoveGroupWithConcurrentConflictingUpdate() throws Exception {
      TestCache testCache = createTestCacheAndReset(GROUP, caches());
      initCache(testCache.primaryOwner);

      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);

      TransactionManager tm = tm(testCache.testCache);
      tm.begin();
      // all keys (and versions) in group stay in context
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
      Transaction tx = tm.suspend();

      testCache.primaryOwner.put(key(1), value(-1));

      tm.resume(tx);
      try {
         testCache.testCache.removeGroup(GROUP);
         expectedGroupSet.clear();
         // all keys in group are removed. It is visible inside the transaction
         assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));

         // removeGroup() conflicts with put(k1, v-1) and a WriteSkewException is expected!
         assertCommitFail(tm);
      } catch (WriteSkewException e) {
         // On non-owner, the second retrieval of keys within the group will find out that one of the entries
         // has different value and will throw WSE
         tm.rollback();
      }

      // transaction rolled back, we should see all keys in group again.
      //noinspection ReuseOfLocalVariable
      expectedGroupSet = createMap(0, 10);
      expectedGroupSet.put(key(1), value(-1));
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
   }

   public void testRemoveGroupWithConcurrentAdd() throws Exception {
      TestCache testCache = createTestCacheAndReset(GROUP, caches());
      initCache(testCache.primaryOwner);

      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);

      TransactionManager tm = tm(testCache.testCache);
      tm.begin();
      // all keys (and versions) in group stay in context
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
      Transaction tx = tm.suspend();

      testCache.primaryOwner.put(key(11), value(11));

      tm.resume(tx);
      // removeGroup sees k11 and it will be removed
      testCache.testCache.removeGroup(GROUP);
      expectedGroupSet.clear();
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
      assertCommitOk(tm); //no write skew expected!

      // no keys in group
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
   }

   public void testRemoveGroupWithConcurrentConflictingRemove() throws Exception {
      TestCache testCache = createTestCacheAndReset(GROUP, caches());
      initCache(testCache.primaryOwner);

      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);

      TransactionManager tm = tm(testCache.testCache);
      tm.begin();
      // all keys (and versions) in group stay in context
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
      Transaction tx = tm.suspend();

      testCache.primaryOwner.remove(key(9));

      tm.resume(tx);
      testCache.testCache.removeGroup(GROUP);
      expectedGroupSet.clear();
      // inside the transaction, no keys should be visible
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
      assertCommitFail(tm); // write skew expected! 2 transactions removed k9 concurrently

      // keys [0,8] not removed
      assertEquals(createMap(0, 9), testCache.testCache.getGroup(GROUP));
   }

   public void testRemoveGroupWithConcurrentRemove() throws Exception {
      TestCache testCache = createTestCacheAndReset(GROUP, caches());
      initCache(testCache.primaryOwner);

      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);

      TransactionManager tm = tm(testCache.testCache);
      tm.begin();
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
      Transaction tx = tm.suspend();

      testCache.primaryOwner.put(key(11), value(11));
      testCache.primaryOwner.put(key(12), value(12));
      testCache.primaryOwner.remove(key(12));

      tm.resume(tx);
      testCache.testCache.removeGroup(GROUP);
      expectedGroupSet.clear();
      // everything is removed including the new keys
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
      assertCommitOk(tm); //write skew should *not* abort the transaction

      // everything is removed
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
   }

   private static void assertCommitFail(TransactionManager tm) throws SystemException {
      try {
         tm.commit();
         fail("Commit should fail!");
      } catch (RollbackException | HeuristicMixedException | HeuristicRollbackException e) {
         //ignored, it is expected
      }
   }

   private static void assertCommitOk(TransactionManager tm) throws Exception {
      tm.commit();
   }
}
