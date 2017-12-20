package org.infinispan.container.versioning;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;

import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.ExceptionRunnable;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "container.versioning.TransactionalLocalWriteSkewTest")
public class TransactionalLocalWriteSkewTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
            .transaction()
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .lockingMode(LockingMode.OPTIMISTIC)
            .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis())
            .isolationLevel(IsolationLevel.REPEATABLE_READ);

      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(builder);
      cacheManager.defineConfiguration("cache", builder.build());
      return cacheManager;
   }

   public void testSharedCounter() throws Exception {
      final int counterMaxValue = 1000;
      Cache<String, Integer> c1 = cacheManager.getCache("cache");

      // initialize the counter
      c1.put("counter", 0);

      // check if the counter is initialized in all caches
      assertEquals(Integer.valueOf(0), c1.get("counter"));

      // this will keep the values put by both threads. any duplicate value
      // will be detected because of the
      // return value of add() method
      ConcurrentSkipListSet<Integer> uniqueValuesIncremented = new ConcurrentSkipListSet<>();

      // create both threads (simulate a node)
      Future<Void> ict1 = fork(new IncrementCounterTask(c1, uniqueValuesIncremented, counterMaxValue));
      Future<Void> ict2 = fork(new IncrementCounterTask(c1, uniqueValuesIncremented, counterMaxValue));

      try {
         // wait to finish
         ict1.get();
         ict2.get();

         // check if all caches obtains the counter_max_values
         assertTrue(c1.get("counter") >= counterMaxValue);
      } finally {
         ict1.cancel(true);
         ict2.cancel(true);
      }
   }

   private class IncrementCounterTask implements ExceptionRunnable {
      private Cache<String, Integer> cache;
      private ConcurrentSkipListSet<Integer> uniqueValuesSet;
      private TransactionManager transactionManager;
      private int lastValue;
      private int counterMaxValue;

      public IncrementCounterTask(Cache<String, Integer> cache, ConcurrentSkipListSet<Integer> uniqueValuesSet, int counterMaxValue) {
         this.cache = cache;
         this.transactionManager = cache.getAdvancedCache().getTransactionManager();
         this.uniqueValuesSet = uniqueValuesSet;
         this.lastValue = 0;
         this.counterMaxValue = counterMaxValue;
      }

      @Override
      public void run() {
         int failuresCounter = 0;
         while (lastValue < counterMaxValue && !Thread.interrupted()) {
            boolean success = false;
            try {
               //start transaction, get the counter value, increment and put it again
               //check for duplicates in case of success
               transactionManager.begin();

               Integer value = cache.get("counter");
               value = value + 1;
               lastValue = value;

               cache.put("counter", value);

               transactionManager.commit();
               success = true;

               boolean unique = uniqueValuesSet.add(value);
               assertTrue("Duplicate value found (value=" + lastValue + ")", unique);
            } catch (Exception e) {
               // expected exception
               failuresCounter++;
               assertTrue("Too many failures incrementing the counter", failuresCounter < 10 * counterMaxValue);
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
            }
         }
      }
   }
}
