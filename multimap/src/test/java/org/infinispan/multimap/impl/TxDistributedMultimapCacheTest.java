package org.infinispan.multimap.impl;

import static java.util.Arrays.asList;
import static org.infinispan.multimap.impl.MultimapTestUtils.JULIEN;
import static org.infinispan.multimap.impl.MultimapTestUtils.NAMES_KEY;
import static org.infinispan.multimap.impl.MultimapTestUtils.assertContaisKeyValue;
import static org.infinispan.multimap.impl.MultimapTestUtils.assertMultimapCacheSize;
import static org.infinispan.multimap.impl.MultimapTestUtils.putValuesOnMultimapCache;
import static org.infinispan.transaction.LockingMode.OPTIMISTIC;
import static org.infinispan.transaction.LockingMode.PESSIMISTIC;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.multimap.api.MultimapCache;
import org.infinispan.test.data.Person;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.TxDistributedMultimapCacheTest")
public class TxDistributedMultimapCacheTest extends DistributedMultimapCacheTest {

   public TxDistributedMultimapCacheTest() {
      transactional = true;
      cleanup = CleanupPhase.AFTER_TEST;
      cacheMode = CacheMode.DIST_SYNC;
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "fromOwner");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), fromOwner ? Boolean.TRUE : Boolean.FALSE);
   }

   @Override
   public Object[] factory() {
      List testsToRun = new ArrayList();
      testsToRun.addAll(txTests(OPTIMISTIC));
      testsToRun.addAll(txTests(PESSIMISTIC));
      return testsToRun.toArray();
   }

   public void testExplicitTx() throws SystemException, NotSupportedException {
      initAndTest();
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);

      TransactionManager tm1 = MultimapTestUtils.getTransactionManager(multimapCache);
      assertMultimapCacheSize(multimapCache, 1);

      tm1.begin();

      try {
         putValuesOnMultimapCache(multimapCache, NAMES_KEY, JULIEN);
         if (fromOwner) {
            assertContaisKeyValue(multimapCache, NAMES_KEY, JULIEN);
         }
         tm1.commit();
      } catch (Exception e) {
         fail(e.getMessage());
      }
      assertValuesAndOwnership(NAMES_KEY, JULIEN);
      assertMultimapCacheSize(multimapCache, 2);
   }

   public void testExplicitTxWithRollback() throws SystemException, NotSupportedException {
      initAndTest();
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);

      TransactionManager tm1 = MultimapTestUtils.getTransactionManager(multimapCache);
      assertMultimapCacheSize(multimapCache, 1);

      tm1.begin();
      try {
         putValuesOnMultimapCache(multimapCache, NAMES_KEY, JULIEN);
         if (fromOwner) {
            assertContaisKeyValue(multimapCache, NAMES_KEY, JULIEN);
         }
      } finally {
         tm1.rollback();
      }
      assertKeyValueNotFoundInAllCaches(NAMES_KEY, JULIEN);
      assertMultimapCacheSize(multimapCache, 1);
   }

   private List txTests(LockingMode lockingMode) {
      return asList(
            new TxDistributedMultimapCacheTest()
                  .fromOwner(false)
                  .lockingMode(lockingMode)
                  .isolationLevel(IsolationLevel.READ_COMMITTED),
            new TxDistributedMultimapCacheTest()
                  .fromOwner(true)
                  .lockingMode(lockingMode)
                  .isolationLevel(IsolationLevel.READ_COMMITTED),
            new TxDistributedMultimapCacheTest()
                  .fromOwner(false)
                  .lockingMode(lockingMode).
                  isolationLevel(IsolationLevel.REPEATABLE_READ),
            new TxDistributedMultimapCacheTest()
                  .fromOwner(true)
                  .lockingMode(lockingMode).
                  isolationLevel(IsolationLevel.REPEATABLE_READ)
      );
   }
}
