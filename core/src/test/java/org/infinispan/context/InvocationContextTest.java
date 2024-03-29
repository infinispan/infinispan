package org.infinispan.context;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestBlocking;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

@Test(groups = {"functional"}, testName = "context.InvocationContextTest")
public class InvocationContextTest extends MultipleCacheManagersTest {
   private static final Log log = LogFactory.getLog(InvocationContextTest.class);

   public InvocationContextTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder.transaction()
            .lockingMode(LockingMode.PESSIMISTIC)
            // TODO: Default values have for synchronization and recovery have changed
            // These two calls are requires to make test behave as before
            // (more info: https://issues.jboss.org/browse/ISPN-2651)
            .useSynchronization(false)
            .recovery().enabled(false);

      createClusteredCaches(1, "timestamps", builder);

      // Keep old configuration commented as reference for ISPN-2651
      //
      // Configuration cfg = TestCacheManagerFactory.getDefaultConfiguration(true);
      // cfg.setSyncCommitPhase(true);
      // cfg.setSyncRollbackPhase(true);
      // cfg.fluent().transaction().lockingMode(LockingMode.PESSIMISTIC);
      // createClusteredCaches(1, "timestamps", cfg);
   }

   public void testMishavingListenerResumesContext() {
      Cache<String, String> cache = cache(0, "timestamps");
      cache.addListener(new CacheListener());
      try {
         cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).put("k", "v");
         fail("Should have failed with an exception");
      } catch (CacheException ce) {
         Throwable cause = ce.getCause();
         assertTrue("Unexpected exception cause " + cause,
               cause instanceof RollbackException || cause instanceof HeuristicRollbackException);
      }
   }

   public void testThreadInterruptedDuringLocking() throws Throwable {
      final Cache<String, String> cache = cache(0, "timestamps");
      cache.put("k", "v");
      // now acquire a lock on k so that subsequent threads will block
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      tm.begin();
      cache.put("k", "v2");
      Transaction tx = tm.suspend();
      final List<Throwable> throwables = new LinkedList<>();

      Thread th = new Thread(() -> {
         try {
            cache.put("k", "v3");
         } catch (Throwable th1) {
            throwables.add(th1);
         }
      });

      th.start();
      // th will now block trying to acquire the lock.
      th.interrupt();
      th.join();
      tm.resume(tx);
      tm.rollback();

      assertEquals(1, throwables.size());
      assertTrue(throwables.get(0) instanceof CacheException);
      assertTrue(throwables.get(0).getCause() instanceof InterruptedException);
   }


   public void testThreadInterruptedAfterLocking() throws Throwable {
      final Cache<String, String> cache = cache(0, "timestamps");
      cache.put("k", "v");
      CountDownLatch willTimeoutLatch = new CountDownLatch(1);
      CountDownLatch lockAquiredSignal = new CountDownLatch(1);
      DelayingListener dl = new DelayingListener(lockAquiredSignal, willTimeoutLatch);
      cache.addListener(dl);
      final List<Throwable> throwables = new LinkedList<>();

      Future<?> future = fork(() -> {
         try {
            cache.put("k", "v3");
         } catch (Throwable th) {
            throwables.add(th);
         }
      });

      // wait for th to acquire the lock
      lockAquiredSignal.await();

      // and now throw the exception
      dl.waitLatch.countDown();
      future.get(10, SECONDS);

      assert throwables.size() == 1;
      assert throwables.get(0) instanceof CacheException;
   }

   @Listener
   public static class DelayingListener {
      CountDownLatch lockAcquiredLatch, waitLatch;

      public DelayingListener(CountDownLatch lockAcquiredLatch, CountDownLatch waitLatch) {
         this.lockAcquiredLatch = lockAcquiredLatch;
         this.waitLatch = waitLatch;
      }

      @CacheEntryModified
      @SuppressWarnings("unused")
      public void entryModified(CacheEntryModifiedEvent event) {
         if (!event.isPre()) {
            lockAcquiredLatch.countDown();
            try {
               TestBlocking.await(waitLatch, 10, SECONDS);
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
            throw new RuntimeException("Induced exception");
         }
      }
   }

   @Listener
   public static class CacheListener {
      @CacheEntryCreated
      @CacheEntryModified
      @SuppressWarnings("unused")
      public void entryModified(CacheEntryEvent event) {
         if (!event.isPre()) {
            log.debugf("Entry modified: %s, let's throw an exception!!", event);
            throw new RuntimeException("Testing exception handling");
         }
      }
   }
}
