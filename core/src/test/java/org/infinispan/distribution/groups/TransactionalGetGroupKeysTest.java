package org.infinispan.distribution.groups;

import java.util.Map;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * It tests the grouping advanced interface for transactional caches.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "distribution.groups.TransactionalGetGroupKeysTest")
public class TransactionalGetGroupKeysTest extends GetGroupKeysTest {
   @Override
   public Object[] factory() {
      return new Object[] {
         new TransactionalGetGroupKeysTest(TestCacheFactory.PRIMARY_OWNER).totalOrder(false).isolationLevel(IsolationLevel.READ_COMMITTED),
         new TransactionalGetGroupKeysTest(TestCacheFactory.PRIMARY_OWNER).totalOrder(true).isolationLevel(IsolationLevel.READ_COMMITTED),
         new TransactionalGetGroupKeysTest(TestCacheFactory.BACKUP_OWNER).totalOrder(false).isolationLevel(IsolationLevel.READ_COMMITTED),
         new TransactionalGetGroupKeysTest(TestCacheFactory.BACKUP_OWNER).totalOrder(true).isolationLevel(IsolationLevel.READ_COMMITTED),
         new TransactionalGetGroupKeysTest(TestCacheFactory.NON_OWNER).totalOrder(false).isolationLevel(IsolationLevel.READ_COMMITTED),
         new TransactionalGetGroupKeysTest(TestCacheFactory.NON_OWNER).totalOrder(true).isolationLevel(IsolationLevel.READ_COMMITTED),
      };
   }

   public TransactionalGetGroupKeysTest() {
      this(null);
   }

   protected TransactionalGetGroupKeysTest(TestCacheFactory factory) {
      super(true, factory);
   }

   public void testGetGroupsInTransaction() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
      final TestCache testCache = createTestCacheAndReset(GROUP, this.caches());
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
      final TestCache testCache = createTestCacheAndReset(GROUP, this.caches());
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
      final TestCache testCache = createTestCacheAndReset(GROUP, this.caches());
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
      final TestCache testCache = createTestCacheAndReset(GROUP, this.caches());
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

      if (isolationLevel == IsolationLevel.READ_COMMITTED) {
         //in ReadCommitted the entries are not wrapped (for read). So the changes are made immediately visible.
         expectedGroupSet.put(key(1), value(-1));
         // GetKeysInGroupCommand will be always replicated to owner nodes and will return all entries, including
         // those we already have in context. With RC, the context gets overwritten by these updated values.
         // If we ran regular testCache.get(key(1)), it would still return just v1 (we wouldn't fetch remote value)
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
      builder.locking().isolationLevel(isolationLevel);
      builder.transaction().transactionProtocol(totalOrder ? TransactionProtocol.TOTAL_ORDER : TransactionProtocol.DEFAULT);
      builder.transaction().recovery().disable();
      return builder;
   }
}
