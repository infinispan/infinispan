package org.infinispan.tx;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import jakarta.transaction.TransactionManager;

/**
 * Test that verifies that Cache.stop() waits for on-going transactions to
 * finish before making the cache unavailable.
 *
 * Also verifies that new transactions started while the cache is stopping
 * are not accepted.
 *
 * @author Galder Zamarreño
 * @author Dan Berindei
 * @since 4.2
 */
@Test(groups = "functional", testName = "tx.TerminatedCacheWhileInTxTest")
public class TerminatedCacheWhileInTxTest extends SingleCacheManagerTest {

   protected StorageType storage;

   @Factory
   public Object[] factory() {
      return new Object[] {
            new TerminatedCacheWhileInTxTest().withStorage(StorageType.BINARY),
            new TerminatedCacheWhileInTxTest().withStorage(StorageType.HEAP),
            new TerminatedCacheWhileInTxTest().withStorage(StorageType.OFF_HEAP)
      };
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder c = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      c.transaction().cacheStopTimeout(10000);
      c.memory().storage(storage);
      return TestCacheManagerFactory.createCacheManager(c);
   }

   public TerminatedCacheWhileInTxTest withStorage(StorageType storage) {
      this.storage = storage;
      return this;
   }

   @Override
   protected String parameters() {
      return "[storage=" + storage + "]";
   }

   /**
    * The aim of this test is to make sure that invocations not belonging to
    * on-going transactions or non-transactional invocations are not allowed
    * once the cache is in stopping mode.
    */
   public void testNotAllowCallsWhileStopping(final Method m) throws Throwable {
      cacheManager.defineConfiguration("cache-" + m.getName(), cacheManager.getDefaultCacheConfiguration());
      final Cache<String, String> cache1 = cacheManager.getCache("cache-" + m.getName());
      final CyclicBarrier barrier = new CyclicBarrier(2);
      final CountDownLatch latch = new CountDownLatch(1);
      final TransactionManager tm = TestingUtil.getTransactionManager(cache1);

      Future<Void> waitAfterModFuture = fork(() -> {
         log.debug("Wait for all executions paths to be ready to perform calls.");
         tm.begin();
         cache1.put(k(m, 1), v(m, 1));
         log.debug("Cache modified, wait for cache to be stopped.");
         barrier.await();
         // Delay the commit, but it must still happen while cache.stop() is waiting for transactions
         assertFalse(latch.await(5, TimeUnit.SECONDS));
         tm.commit();
         return null;
      });

      // wait for the transaction to have started
      barrier.await();

      Future<Void> callStoppingCacheFuture = fork(() -> {
         log.debug("Wait very briefly and then make call.");
         Thread.sleep(2000);
         cache1.put(k(m, 2), v(m, 2));
         return null;
      });

      cache1.stop(); // now stop the cache
      latch.countDown(); // now that cache has been stopped, let the thread continue

      waitAfterModFuture.get();
      try {
         callStoppingCacheFuture.get();
         fail("Should have thrown an IllegalLifecycleStateException");
      } catch (ExecutionException e) {
         assertTrue(e.toString(), e.getCause() instanceof IllegalLifecycleStateException);
      }
   }
}
