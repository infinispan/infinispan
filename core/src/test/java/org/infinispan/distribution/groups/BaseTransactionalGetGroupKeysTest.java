package org.infinispan.distribution.groups;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Map;

/**
 * It tests the grouping advanced interface for transactional caches.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional")
public abstract class BaseTransactionalGetGroupKeysTest extends BaseGetGroupKeysTest {


   protected BaseTransactionalGetGroupKeysTest(TestCacheFactory factory) {
      super(true, factory);
   }

   public void testGetGroupsInTransaction() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
      final TestCache testCache = createTestCacheAndReset(GROUP, this.<GroupKey, String>caches());
      initCache(testCache.primaryOwner);

      final TransactionManager tm = tm(testCache.testCache);
      tm.begin();
      testCache.testCache.put(key(10), value(10));
      testCache.testCache.put(key(11), value(11));
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 12);
      tm.commit();

      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);

      groupKeySet = testCache.testCache.getGroup(GROUP);
      expectedGroupSet = createMap(0, 12);
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
   }

   public void testGetGroupsWithConcurrentPut() throws Exception {
      final TestCache testCache = createTestCacheAndReset(GROUP, this.<GroupKey, String>caches());
      initCache(testCache.primaryOwner);

      final TransactionManager tm = tm(testCache.testCache);
      tm.begin();
      testCache.testCache.put(key(10), value(10));
      testCache.testCache.put(key(11), value(11));
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 12);
      final Transaction tx = tm.suspend();

      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);

      testCache.primaryOwner.put(key(12), value(12));
      expectedGroupSet.put(key(12), value(12));

      tm.resume(tx);
      groupKeySet = testCache.testCache.getGroup(GROUP);
      tm.commit();

      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);

      groupKeySet = testCache.testCache.getGroup(GROUP);
      expectedGroupSet = createMap(0, 13);
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
   }

   public void testGetGroupsWithConcurrentRemove() throws Exception {
      final TestCache testCache = createTestCacheAndReset(GROUP, this.<GroupKey, String>caches());
      initCache(testCache.primaryOwner);

      final TransactionManager tm = tm(testCache.testCache);
      tm.begin();
      testCache.testCache.put(key(10), value(10));
      testCache.testCache.put(key(11), value(11));
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 12);
      final Transaction tx = tm.suspend();

      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);

      testCache.primaryOwner.remove(key(1));

      tm.resume(tx);
      groupKeySet = testCache.testCache.getGroup(GROUP);
      tm.commit();

      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);

      groupKeySet = testCache.testCache.getGroup(GROUP);
      expectedGroupSet.remove(key(1));
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
   }

   public void testGetGroupsWithConcurrentReplace() throws Exception {
      final TestCache testCache = createTestCacheAndReset(GROUP, this.<GroupKey, String>caches());
      initCache(testCache.primaryOwner);

      final TransactionManager tm = tm(testCache.testCache);
      tm.begin();
      testCache.testCache.put(key(10), value(10));
      testCache.testCache.put(key(11), value(11));
      Map<GroupKey, String> groupKeySet = testCache.testCache.getGroup(GROUP);
      Map<GroupKey, String> expectedGroupSet = createMap(0, 12);
      final Transaction tx = tm.suspend();

      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);

      testCache.primaryOwner.put(key(1), value(-1));

      if ((factory == TestCacheFactory.PRIMARY_OWNER || factory == TestCacheFactory.BACKUP_OWNER) &&
            getIsolationLevel() == IsolationLevel.READ_COMMITTED) {
         //in ReadCommitted the entries are not wrapped (for read). So the changes are made immediately visible.
         expectedGroupSet.put(key(1), value(-1));
      }

      tm.resume(tx);
      groupKeySet = testCache.testCache.getGroup(GROUP);
      tm.commit();

      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);

      groupKeySet = testCache.testCache.getGroup(GROUP);
      expectedGroupSet.put(key(1), value(-1));
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
   }


   @Override
   protected ConfigurationBuilder amendConfiguration(ConfigurationBuilder builder) {
      super.amendConfiguration(builder);
      builder.locking().isolationLevel(getIsolationLevel()).writeSkewCheck(false);
      builder.versioning().disable();
      builder.transaction().transactionProtocol(getTransactionProtocol());
      builder.transaction().recovery().disable();
      return builder;
   }

   protected abstract IsolationLevel getIsolationLevel();

   protected abstract TransactionProtocol getTransactionProtocol();
}

