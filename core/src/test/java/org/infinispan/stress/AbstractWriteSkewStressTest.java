package org.infinispan.stress;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import javax.transaction.Status;
import javax.transaction.TransactionManager;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.AssertJUnit.*;

/**
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(testName = "stress.AbstractWriteSkewStressTest", groups = "stress")
public abstract class AbstractWriteSkewStressTest extends MultipleCacheManagersTest {

   private static final String SHARED_COUNTER_TEST_KEY = "counter";
   private static final int SHARED_COUNTER_TEST_MAX_COUNTER_VALUE = 1000;

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
            .lockAcquisitionTimeout(100)
            .writeSkewCheck(true)
            .transaction()
            .lockingMode(LockingMode.OPTIMISTIC)
            .syncCommitPhase(true);

      decorate(builder);

      createCluster(builder, 2);
      waitForClusterToForm();
   }

   protected void decorate(ConfigurationBuilder builder) {
      // No-op
   }

   protected abstract CacheMode getCacheMode();

   // This test is based on a contribution by Pedro Ruivo of INESC-ID, working on the Cloud-TM project.
   public void testSharedCounter() {
      final Cache<String, Integer> c1 = cache(0);
      final Cache<String, Integer> c2 = cache(1);

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
         assertTrue("Cache 1 [" + address(c1) + "] has put a duplicate value", f1.get(5, TimeUnit.MINUTES));
         assertTrue("Cache 2 [" + address(c2) + "] has put a duplicate value", f2.get(5, TimeUnit.MINUTES));
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

}
