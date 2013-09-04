package org.infinispan.container.versioning;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

@Test(testName = "container.versioning.AbstractClusteredWriteSkewTest", groups = "functional")
public abstract class AbstractClusteredWriteSkewTest extends MultipleCacheManagersTest {

   private static final String SHARED_COUNTER_TEST_CACHE_NAME = "shared_counter_test";
   private static final String SHARED_COUNTER_TEST_KEY = "counter";
   private static final int SHARED_COUNTER_TEST_MAX_COUNTER_VALUE = 100;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);

      builder
            .clustering()
            .cacheMode(getCacheMode())
            .versioning()
            .enable()
            .scheme(VersioningScheme.SIMPLE)
            .locking()
            .isolationLevel(IsolationLevel.REPEATABLE_READ)
            .writeSkewCheck(true)
            .transaction()
            .lockingMode(LockingMode.OPTIMISTIC)
            .syncCommitPhase(true);

      decorate(builder);

      createCluster(builder, clusterSize());
      waitForClusterToForm();

      builder.locking().lockAcquisitionTimeout(100);
      defineConfigurationOnAllManagers(SHARED_COUNTER_TEST_CACHE_NAME, builder);
   }

   protected void decorate(ConfigurationBuilder builder) {
      // No-op
   }

   protected abstract CacheMode getCacheMode();

   protected abstract int clusterSize();

   public final void testPutIgnoreReturnValueOnNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.PUT, false);
   }

   public final void testPutIgnoreReturnValueOnNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.PUT, false);
   }

   public final void testPutIgnoreReturnValueNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.PUT, true);
   }

   public final void testPutIgnoreReturnValueNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.PUT, true);
   }

   public final void testRemoveIgnoreReturnValueOnNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.REMOVE, false);
   }

   public final void testRemoveIgnoreReturnValueOnNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.REMOVE, false);
   }

   public final void testRemoveIgnoreReturnValueNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.REMOVE, true);
   }

   public final void testRemoveIgnoreReturnValueNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.REMOVE, true);
   }

   public final void testReplaceIgnoreReturnValueOnNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.REPLACE, false);
   }

   public final void testReplaceIgnoreReturnValueOnNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.REPLACE, false);
   }

   public final void testReplaceIgnoreReturnValueNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.REPLACE, true);
   }

   public final void testReplaceIgnoreReturnValueNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.REPLACE, true);
   }

   public final void testPutIfAbsentIgnoreReturnValueOnNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.CONDITIONAL_PUT, false);
   }

   public final void testPutIfAbsentIgnoreReturnValueOnNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.CONDITIONAL_PUT, false);
   }

   public final void testPutIfAbsentIgnoreReturnValueNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.CONDITIONAL_PUT, true);
   }

   public final void testPutIfAbsentIgnoreReturnValueNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.CONDITIONAL_PUT, true);
   }

   public final void testConditionalRemoveIgnoreReturnValueOnNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.CONDITIONAL_REMOVE, false);
   }

   public final void testConditionalRemoveIgnoreReturnValueOnNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.CONDITIONAL_REMOVE, false);
   }

   public final void testConditionalRemoveIgnoreReturnValueNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.CONDITIONAL_REMOVE, true);
   }

   public final void testConditionalRemoveIgnoreReturnValueNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.CONDITIONAL_REMOVE, true);
   }

   public final void testConditionalReplaceIgnoreReturnValueOnNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.CONDITIONAL_REPLACE, false);
   }

   public final void testConditionalReplaceIgnoreReturnValueOnNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.CONDITIONAL_REPLACE, false);
   }

   public final void testConditionalReplaceIgnoreReturnValueNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.CONDITIONAL_REPLACE, true);
   }

   public final void testConditionalReplaceIgnoreReturnValueNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.CONDITIONAL_REPLACE, true);
   }

   private void doIgnoreReturnValueTest(boolean executeOnPrimaryOwner, Operation operation, boolean initKey) throws Exception {
      final Object key = new MagicKey("ignore-return-value", cache(0));
      final AdvancedCache<Object, Object> c = executeOnPrimaryOwner ? advancedCache(0) : advancedCache(1);
      final TransactionManager tm = executeOnPrimaryOwner ? tm(0) : tm(1);

      for (Cache cache : caches()) {
         AssertJUnit.assertNull("wrong initial value for " + address(cache) + ".", cache.get(key));
      }

      log.debugf("Initialize the key? %s", initKey);
      if (initKey) {
         cache(0).put(key, "init");
      }

      Object finalValue = null;
      boolean rollbackExpected = false;

      log.debugf("Start the transaction and perform a %s operation", operation);
      tm.begin();
      switch (operation) {
         case PUT:
            finalValue = "v1";
            rollbackExpected = false;
            c.withFlags(Flag.IGNORE_RETURN_VALUES).put(key, "v1");
            break;
         case REMOVE:
            finalValue = null;
            rollbackExpected = false;
            c.withFlags(Flag.IGNORE_RETURN_VALUES).remove(key);
            break;
         case REPLACE:
            finalValue = "v2";
            rollbackExpected = true;
            c.withFlags(Flag.IGNORE_RETURN_VALUES).replace(key, "v1");
            break;
         case CONDITIONAL_PUT:
            finalValue = "v2";
            rollbackExpected = true;
            c.withFlags(Flag.IGNORE_RETURN_VALUES).putIfAbsent(key, "v1");
            break;
         case CONDITIONAL_REMOVE:
            finalValue = "v2";
            rollbackExpected = true;
            c.withFlags(Flag.IGNORE_RETURN_VALUES).remove(key, "init");
            break;
         case CONDITIONAL_REPLACE:
            finalValue = "v2";
            rollbackExpected = true;
            c.withFlags(Flag.IGNORE_RETURN_VALUES).replace(key, "init", "v1");
            break;
         default:
            tm.rollback();
            fail("Unknown operation " + operation);
      }
      Transaction tx = tm.suspend();

      log.debugf("Suspend the transaction and update the key");
      c.put(key, "v2");

      log.debugf("Checking if all the keys has the same value");
      for (Cache cache : caches()) {
         assertEquals("wrong intermediate value for " + address(cache) + ".", "v2", cache.get(key));
      }

      log.debugf("It is going to try to commit the suspended transaction");
      try {
         tm.resume(tx);
         tm.commit();
         if (rollbackExpected) {
            fail("Rollback expected!");
         }
      } catch (RollbackException e) {
         if (!rollbackExpected) {
            fail("Rollback *not* expected!");
         }
         //no-op
      }

      log.debugf("So far so good. Check the key final value");
      assertNoTransactions();
      for (Cache cache : caches()) {
         assertEquals("wrong final value for " + address(cache) + ".", finalValue, cache.get(key));
      }
   }

   // This test is based on a contribution by Pedro Ruivo of INESC-ID, working on the Cloud-TM project.
   public void testSharedCounter() {
      waitForClusterToForm(SHARED_COUNTER_TEST_CACHE_NAME);
      final Cache<String, Integer> c1 = cache(0, SHARED_COUNTER_TEST_CACHE_NAME);
      final Cache<String, Integer> c2 = cache(1, SHARED_COUNTER_TEST_CACHE_NAME);

      //initialize the counter
      c1.put(SHARED_COUNTER_TEST_KEY, 0);

      //check if the counter is initialized in all caches
      assertEquals("Initial value is different from zero in cache 1", 0, (int) c1.get(SHARED_COUNTER_TEST_KEY));
      assertEquals("Initial value is different from zero in cache 2", 0, (int) c2.get(SHARED_COUNTER_TEST_KEY));

      //this will keep the values put by both threads. any duplicate value will be detected because of the
      //return value of add() method
      final Set<Integer> uniqueValuesIncremented = new ConcurrentSkipListSet<Integer>();

      //create both threads (each of them incrementing the counter on one node)
      Future<Boolean> f1 = fork(new IncrementCounterTask(c1, uniqueValuesIncremented));
      Future<Boolean> f2 = fork(new IncrementCounterTask(c2, uniqueValuesIncremented));

      try {
         // wait to finish and check is any duplicate value has been detected
         assertTrue("Cache 1 [" + address(c1) + "] has put a duplicate value", f1.get(30, TimeUnit.SECONDS));
         assertTrue("Cache 2 [" + address(c2) + "] has put a duplicate value", f2.get(30, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
         fail("Interrupted exception while running the test");
      } catch (ExecutionException e) {
         log.error("Exception in running updater threads", e);
         fail("Exception running updater threads");
      } catch (TimeoutException e) {
         fail("Timed out waiting for updater threads");
      } finally {
         f1.cancel(true);
         f2.cancel(true);
      }

      //check if all caches obtains the counter_max_values
      assertTrue("Cache 1 [" + address(c1) + "] fina value is less than " + SHARED_COUNTER_TEST_MAX_COUNTER_VALUE,
                 c1.get(SHARED_COUNTER_TEST_KEY) >= SHARED_COUNTER_TEST_MAX_COUNTER_VALUE);
      assertTrue("Cache 2 [" + address(c2) + "] fina value is less than " + SHARED_COUNTER_TEST_MAX_COUNTER_VALUE,
                 c2.get(SHARED_COUNTER_TEST_KEY) >= SHARED_COUNTER_TEST_MAX_COUNTER_VALUE);
   }

   private class IncrementCounterTask implements Callable<Boolean> {
      private final Cache<String, Integer> cache;
      private final Set<Integer> uniqueValuesSet;
      private final TransactionManager transactionManager;
      private int lastValue;

      public IncrementCounterTask(Cache<String, Integer> cache, Set<Integer> uniqueValuesSet) {
         this.cache = cache;
         this.transactionManager = cache.getAdvancedCache().getTransactionManager();
         this.uniqueValuesSet = uniqueValuesSet;
         this.lastValue = 0;

      }

      @Override
      public Boolean call() throws InterruptedException {
         boolean unique = true;
         while (lastValue < SHARED_COUNTER_TEST_MAX_COUNTER_VALUE && !Thread.interrupted()) {
            boolean success = false;
            try {
               //start transaction, get the counter value, increment and put it again
               //check for duplicates in case of success
               transactionManager.begin();

               Integer value = cache.get(SHARED_COUNTER_TEST_KEY);
               value = value + 1;
               lastValue = value;

               cache.put(SHARED_COUNTER_TEST_KEY, value);

               transactionManager.commit();

               unique = uniqueValuesSet.add(value);
               success = true;
            } catch (Exception e) {
               // expected exception
            } finally {
               if (!success) {
                  try {
                     //lets rollback
                     if (transactionManager.getStatus() != Status.STATUS_NO_TRANSACTION)
                        transactionManager.rollback();
                  } catch (Throwable t) {
                     //the only possible exception is thrown by the rollback. just ignore it
                     log.trace("Exception during rollback", t);
                  }
               }
               assertTrue("Duplicate value found in " + address(cache) + " (value=" + lastValue + ")", unique);
            }
         }
         return unique;
      }
   }

   private static enum Operation {
      PUT, REMOVE, REPLACE,
      CONDITIONAL_PUT, CONDITIONAL_REMOVE, CONDITIONAL_REPLACE
   }
}
