package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

/**
 * Test that verifies that Cache.stop() waits for on-going transactions to
 * finish before making the cache unavailable.
 *
 * @author Galder Zamarreño
 * @since 4.2
 * @since 5.0
 */
@Test(groups = "functional", testName = "tx.TerminatedCacheWhileInTxTest")
public class TerminatedCacheWhileInTxTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder c = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      return TestCacheManagerFactory.createCacheManager(c);
   }

   public void testStopWhileInTx(Method m) throws Throwable {
      stopCacheCalls(m, false);
   }

   /**
    * The aim of this test is to make sure that invocations not belonging to
    * on-going transactions or non-transactional invocations are not allowed
    * once the cache is in stopping mode.
    */
   @Test(expectedExceptions = IllegalStateException.class)
   public void testNotAllowCallsWhileStopping(Method m) throws Throwable {
      stopCacheCalls(m, true);
   }

   private void stopCacheCalls(final Method m, boolean withCallStoppingCache) throws Throwable {
      final Cache<String, String> cache = cacheManager.getCache("cache-" + m.getName());
      final ExecutorService executorService = Executors.newCachedThreadPool();
      final CyclicBarrier barrier = new CyclicBarrier(2);
      final CountDownLatch latch = new CountDownLatch(1);
      final TransactionManager tm = TestingUtil.getTransactionManager(cache);

      Callable<Void> waitAfterModCallable = new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            log.debug("Wait for all executions paths to be ready to perform calls.");
            tm.begin();
            cache.put(k(m, 1), v(m, 1));
            log.debug("Cache modified, wait for cache to be stopped.");
            barrier.await();
            latch.await(10, TimeUnit.SECONDS);
            tm.commit();
            return null;
         }
      };
      Future<Void> waitAfterModFuture = executorService.submit(waitAfterModCallable);

      barrier.await(); // wait for all threads to have done their modifications
      Future<Void> callStoppingCacheFuture = null;
      if (withCallStoppingCache) {
         Callable<Void> callStoppingCache = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
               log.debug("Wait very briefly and then make call.");
               Thread.sleep(1000);
               cache.put(k(m, 2), v(m, 2));
               return null;
            }
         };
         callStoppingCacheFuture = executorService.submit(callStoppingCache);
      }
      cache.stop(); // now stop the cache
      latch.countDown(); // now that cache has been stopped, let the thread continue

      log.debug("All threads finished, let's shutdown the executor and check whether any exceptions were reported");
      waitAfterModFuture.get();
      if (callStoppingCacheFuture != null) {
         try {
            callStoppingCacheFuture.get();
         } catch (ExecutionException e) {
            throw e.getCause();
         }
      }

      executorService.shutdownNow();
   }
}
