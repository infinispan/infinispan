package org.infinispan.distribution.groups;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Map;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.util.concurrent.IsolationLevel;
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
      return new Object[]{
            new TransactionalGetGroupKeysTest(TestCacheFactory.PRIMARY_OWNER).isolationLevel(IsolationLevel.READ_COMMITTED),
            new TransactionalGetGroupKeysTest(TestCacheFactory.BACKUP_OWNER).isolationLevel(IsolationLevel.READ_COMMITTED),
            new TransactionalGetGroupKeysTest(TestCacheFactory.NON_OWNER).isolationLevel(IsolationLevel.READ_COMMITTED),
      };
   }

   public TransactionalGetGroupKeysTest() {
      this(null);
   }

   protected TransactionalGetGroupKeysTest(TestCacheFactory factory) {
      super(true, factory);
   }

   public void testGetGroupsInTransaction() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
      TestCache testCache = createTestCacheAndReset(GROUP, caches());
      initCache(testCache.primaryOwner);

      Map<GroupKey, String> expectedGroupSet = createMap(0, 12);

      TransactionManager tm = tm(testCache.testCache);
      tm.begin();
      testCache.testCache.put(key(10), value(10));
      testCache.testCache.put(key(11), value(11));
      // make sure that uncommitted value are shown in the transaction
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
      tm.commit();

      assertEquals(createMap(0, 12), testCache.testCache.getGroup(GROUP));
   }

   public void testGetGroupsWithConcurrentPut() throws Exception {
      TestCache testCache = createTestCacheAndReset(GROUP, caches());
      initCache(testCache.primaryOwner);

      Map<GroupKey, String> expectedGroupSet = createMap(0, 12);

      TransactionManager tm = tm(testCache.testCache);
      tm.begin();
      testCache.testCache.put(key(10), value(10));
      testCache.testCache.put(key(11), value(11));
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
      Transaction tx = tm.suspend();

      testCache.primaryOwner.put(key(12), value(12));
      expectedGroupSet.put(key(12), value(12));

      tm.resume(tx);
      // k12 is committed, should be visible now
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
      tm.commit();

      // after commit, everything is visible
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
   }

   public void testGetGroupsWithConcurrentRemove() throws Exception {
      TestCache testCache = createTestCacheAndReset(GROUP, caches());
      initCache(testCache.primaryOwner);

      Map<GroupKey, String> expectedGroupSet = createMap(0, 12);

      TransactionManager tm = tm(testCache.testCache);
      tm.begin();
      testCache.testCache.put(key(10), value(10));
      testCache.testCache.put(key(11), value(11));
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
      Transaction tx = tm.suspend();

      testCache.primaryOwner.remove(key(1));

      tm.resume(tx);
      // previous getGroup() read k1, so the remove is not visible
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
      tm.commit();

      // after commit, everything is visible
      expectedGroupSet.remove(key(1));
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
   }

   public void testGetGroupsWithConcurrentReplace() throws Exception {
      TestCache testCache = createTestCacheAndReset(GROUP, caches());
      initCache(testCache.primaryOwner);

      Map<GroupKey, String> expectedGroupSet = createMap(0, 12);

      TransactionManager tm = tm(testCache.testCache);
      tm.begin();
      testCache.testCache.put(key(10), value(10));
      testCache.testCache.put(key(11), value(11));
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
      Transaction tx = tm.suspend();

      testCache.primaryOwner.put(key(1), value(-1));

      if (isolationLevel == IsolationLevel.READ_COMMITTED && factory != TestCacheFactory.NON_OWNER) {
         // in ReadCommitted the entries are not wrapped (for read). So the changes are made immediately visible in write owners
         // non owners, will use the entry in the context
         expectedGroupSet.put(key(1), value(-1));
      }

      tm.resume(tx);
      // cacheStream wraps entries; even with read-committed, we are unable to see entry k1=v-1
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
      tm.commit();

      // after commit, everything is visible
      expectedGroupSet.put(key(1), value(-1));
      assertEquals(expectedGroupSet, testCache.testCache.getGroup(GROUP));
   }


   @Override
   protected ConfigurationBuilder amendConfiguration(ConfigurationBuilder builder) {
      super.amendConfiguration(builder);
      builder.locking().isolationLevel(isolationLevel);
      builder.transaction().recovery().disable();
      return builder;
   }
}
