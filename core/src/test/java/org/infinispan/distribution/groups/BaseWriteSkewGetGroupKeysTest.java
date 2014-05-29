package org.infinispan.distribution.groups;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Map;

/**
 * It tests the grouping advanced interface for transactional caches with write-skew check enabled.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional")
public abstract class BaseWriteSkewGetGroupKeysTest extends BaseTransactionalGetGroupKeysTest {


   protected BaseWriteSkewGetGroupKeysTest(TestCacheFactory factory) {
      super(factory);
   }


   public void testRemoveGroupWithConcurrentConflictingUpdate() throws Exception{
      final TestCache testCache = createTestCacheAndReset(GROUP, this.<GroupKey, String>caches());
      initCache(testCache.primaryOwner);

      final TransactionManager tm = tm(testCache.testCache);
      tm.begin();
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      final Transaction tx = tm.suspend();

      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);

      testCache.primaryOwner.put(key(1), value(-1));

      tm.resume(tx);
      testCache.testCache.removeGroup(GROUP);
      groupKeySet = testCache.testCache.getGroup(GROUP);
      expectedGroupSet.clear();
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
      assertCommitFail(tm); //write skew should abort the transaction


      groupKeySet = testCache.testCache.getGroup(GROUP);
      expectedGroupSet = createMap(0, 10);
      expectedGroupSet.put(key(1), value(-1));
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
   }

   public void testRemoveGroupWithConcurrentAdd() throws Exception{
      final TestCache testCache = createTestCacheAndReset(GROUP, this.<GroupKey, String>caches());
      initCache(testCache.primaryOwner);

      final TransactionManager tm = tm(testCache.testCache);
      tm.begin();
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      final Transaction tx = tm.suspend();

      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);

      testCache.primaryOwner.put(key(11), value(11));

      tm.resume(tx);
      testCache.testCache.removeGroup(GROUP);
      groupKeySet = testCache.testCache.getGroup(GROUP);
      expectedGroupSet.clear();
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
      assertCommitOk(tm); //write skew should *not* abort the transaction

      groupKeySet = testCache.testCache.getGroup(GROUP);
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
   }

   public void testRemoveGroupWithConcurrentConflictingRemove() throws Exception{
      final TestCache testCache = createTestCacheAndReset(GROUP, this.<GroupKey, String>caches());
      initCache(testCache.primaryOwner);

      final TransactionManager tm = tm(testCache.testCache);
      tm.begin();
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      final Transaction tx = tm.suspend();

      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);

      testCache.primaryOwner.remove(key(9));

      tm.resume(tx);
      testCache.testCache.removeGroup(GROUP);
      groupKeySet = testCache.testCache.getGroup(GROUP);
      expectedGroupSet.clear();
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
      assertCommitFail(tm); //write skew should *not* abort the transaction

      groupKeySet = testCache.testCache.getGroup(GROUP);
      expectedGroupSet = createMap(0, 9);
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
   }

   public void testRemoveGroupWithConcurrentRemove() throws Exception{
      final TestCache testCache = createTestCacheAndReset(GROUP, this.<GroupKey, String>caches());
      initCache(testCache.primaryOwner);

      final TransactionManager tm = tm(testCache.testCache);
      tm.begin();
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      final Transaction tx = tm.suspend();

      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);

      testCache.primaryOwner.put(key(11), value(11));
      testCache.primaryOwner.put(key(12), value(12));
      testCache.primaryOwner.remove(key(12));

      tm.resume(tx);
      testCache.testCache.removeGroup(GROUP);
      groupKeySet = testCache.testCache.getGroup(GROUP);
      expectedGroupSet.clear();
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
      assertCommitOk(tm); //write skew should *not* abort the transaction

      groupKeySet = testCache.testCache.getGroup(GROUP);
      expectedGroupSet.clear();
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
   }

   @Override
   protected ConfigurationBuilder amendConfiguration(ConfigurationBuilder builder) {
      super.amendConfiguration(builder);
      builder.locking().writeSkewCheck(true);
      builder.versioning().enable().scheme(VersioningScheme.SIMPLE);
      return builder;
   }

   private static void assertCommitFail(TransactionManager tm) throws SystemException {
      try {
         tm.commit();
         AssertJUnit.fail("Commit should fail!");
      } catch (RollbackException | HeuristicMixedException | HeuristicRollbackException e) {
         //ignored, it is expected
      }
   }

   private static void assertCommitOk(TransactionManager tm) throws SystemException {
      try {
         tm.commit();
      } catch (RollbackException | HeuristicMixedException | HeuristicRollbackException e) {
         AssertJUnit.fail("Commit should fail!");
      }
   }

   protected final IsolationLevel getIsolationLevel() {
      return IsolationLevel.REPEATABLE_READ;
   }

   protected abstract TransactionProtocol getTransactionProtocol();
}

