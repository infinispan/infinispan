package org.infinispan.lock;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.lang.String.valueOf;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Contributed by Dmitry Udalov
 *
 * @author Dmitry Udalov
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "lock.ExplicitUnlockTest")
@CleanupAfterMethod
public class ExplicitUnlockTest extends SingleCacheManagerTest {

   private static final Log log = LogFactory.getLog(ExplicitUnlockTest.class);
   private static final int NUMBER_OF_KEYS = 10;

   public void testLock() throws Exception {
      doTestLock(true, 10, 10);
   }

   public void testLockTwoTasks() throws Exception {
      doTestLock(true, 2, 10);
   }

   public void testLockNoExplicitUnlock() throws Exception {
      doTestLock(false, 10, 10);
   }

   public void testLockNoExplicitUnlockTwoTasks() throws Exception {
      doTestLock(false, 10, 10);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   private void doTestLock(boolean withUnlock, int nThreads, long stepDelayMsec) throws Exception {
      for (int key = 1; key <= NUMBER_OF_KEYS; key++) {
         cache.put("" + key, "value");
      }

      List<Future<Boolean>> results = new ArrayList<Future<Boolean>>(nThreads);

      for (int i = 1; i <= nThreads; i++) {
         results.add(fork(new Worker(i, cache, withUnlock, stepDelayMsec)));
      }

      boolean success = true;
      for (Future<Boolean> next : results) {
         success = success && next.get(30, TimeUnit.SECONDS);
      }
      assertTrue("All worker should complete without exceptions", success);
      assertNoTransactions();
      for (int i = 0; i < NUMBER_OF_KEYS; ++i) {
         assertNotLocked(cache, valueOf(i));
      }
   }

   private static class Worker implements Callable<Boolean> {

      private static String lockKey = "0";     // there is no cached Object with such key
      private final Cache<Object, Object> cache;
      private final boolean withUnlock;
      private final long stepDelayMsec;
      private final int index;

      public Worker(int index, final Cache<Object, Object> cache, boolean withUnlock, long stepDelayMsec) {
         this.index = index;
         this.cache = cache;
         this.withUnlock = withUnlock;
         this.stepDelayMsec = stepDelayMsec;
      }

      @Override
      public Boolean call() throws Exception {
         boolean success;
         try {
            doRun();
            success = true;
         } catch (Throwable t) {
            log.errorf(t, "Error in Worker[%s, unlock? %s]", index, withUnlock);
            success = false;
         }
         return success;
      }

      private void log(String method, String msg) {
         log.debugf("Worker[%s, unlock? %s] %s %s", index, withUnlock, method, msg);
      }

      private void doRun() throws Exception {
         final String methodName = "run";
         TransactionManager mgr = cache.getAdvancedCache().getTransactionManager();
         if (null == mgr) {
            throw new UnsupportedOperationException("TransactionManager was not configured for the cache " + cache.getName());
         }
         mgr.begin();

         try {
            if (acquireLock()) {
               log(methodName, "acquired lock");

               // renaming all Objects from 1 to NUMBER_OF_KEYS
               String newName = "value-" + index;
               log(methodName, "Changing value to " + newName);
               for (int key = 1; key <= NUMBER_OF_KEYS; key++) {
                  cache.put(valueOf(key), newName);
                  Thread.sleep(stepDelayMsec);
               }

               validateCache();

               if (withUnlock) {
                  unlock(lockKey);
               }

            } else {
               log(methodName, "Failed to acquired lock");
            }
            mgr.commit();

         } catch (Exception t) {
            mgr.rollback();
            throw t;
         }
      }

      private boolean acquireLock() {
         return cache.getAdvancedCache().lock(lockKey);
      }

      private boolean unlock(String resourceId) {
         LockManager lockManager = cache.getAdvancedCache().getLockManager();
         Object lockOwner = lockManager.getOwner(resourceId);
         Collection<Object> keys = Collections.<Object>singletonList(resourceId);
         lockManager.unlock(keys, lockOwner);
         return true;
      }

      /**
       * Checks if all cache entries are consistent
       *
       * @throws InterruptedException
       */
      private void validateCache() throws InterruptedException {

         String value = getCachedValue(1);
         for (int key = 1; key <= NUMBER_OF_KEYS; key++) {
            String nextValue = getCachedValue(key);
            if (!value.equals(nextValue)) {
               String msg = String.format("Cache inconsistent: value=%s, nextValue=%s", value, nextValue);
               log("validate_cache", msg);
               throw new ConcurrentModificationException(msg);
            }
            Thread.sleep(stepDelayMsec);
         }
         log("validate_cache", "passed: " + value);
      }

      private String getCachedValue(int index) {
         String value = (String) cache.get(valueOf(index));
         if (null == value) {
            throw new ConcurrentModificationException("Missed entry for " + index);
         }
         return value;
      }
   }


}
