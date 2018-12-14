package org.infinispan.lock;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.test.Exceptions.assertException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockConfiguration;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.exception.ClusteredLockException;
import org.infinispan.lock.logging.Log;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "clusteredLock.ClusteredLockTest")
public class ClusteredLockTest extends BaseClusteredLockTest {

   protected static final String LOCK_NAME = "ClusteredLockTest";

   public ClusteredLockTest() {
      super();
      cacheMode = CacheMode.REPL_SYNC;
   }

   @BeforeMethod(alwaysRun = true)
   public void createLock() throws Throwable {
      ClusteredLockManager m1 = clusteredLockManager(0);
      m1.defineLock(LOCK_NAME, new ClusteredLockConfiguration());
   }

   @AfterMethod(alwaysRun = true)
   protected void destroyLock() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      await(clusteredLockManager.remove(LOCK_NAME));
   }

   @Test
   public void testLockAndUnlockVisibility() throws Throwable {
      ClusteredLockManager cm0 = clusteredLockManager(0);
      ClusteredLockManager cm1 = clusteredLockManager(1);
      ClusteredLockManager cm2 = clusteredLockManager(2);
      ClusteredLock lock0 = cm0.get(LOCK_NAME);
      ClusteredLock lock1 = cm1.get(LOCK_NAME);
      ClusteredLock lock2 = cm2.get(LOCK_NAME);

      // Is not locked
      assertFalse(await(lock0.isLocked()));
      assertFalse(await(lock1.isLocked()));
      assertFalse(await(lock2.isLocked()));

      // lock0 from cm0 locks
      await(lock0.lock());

      // Is locked by everybody
      assertTrue(await(lock0.isLocked()));
      assertTrue(await(lock1.isLocked()));
      assertTrue(await(lock2.isLocked()));

      // lock1 from cm1 tries to unlock unsuccessfully
      await(lock1.unlock());
      assertTrue(await(lock0.isLocked()));
      assertTrue(await(lock1.isLocked()));
      assertTrue(await(lock2.isLocked()));

      // lock2 from cm2 tries to unlock unsuccessfully
      await(lock2.unlock());
      assertTrue(await(lock0.isLocked()));
      assertTrue(await(lock1.isLocked()));
      assertTrue(await(lock2.isLocked()));

      // lock0 from cm0 tries to unlock successfully
      await(lock0.unlock());
      assertFalse(await(lock0.isLocked()));
      assertFalse(await(lock1.isLocked()));
      assertFalse(await(lock2.isLocked()));
   }

   @Test
   public void testLockOwnership() throws Throwable {
      ClusteredLockManager cm0 = clusteredLockManager(0);
      ClusteredLockManager cm1 = clusteredLockManager(1);
      ClusteredLockManager cm2 = clusteredLockManager(2);
      ClusteredLock lock0 = cm0.get(LOCK_NAME);
      ClusteredLock lock1 = cm1.get(LOCK_NAME);
      ClusteredLock lock2 = cm2.get(LOCK_NAME);

      // nobody owns the lock
      assertFalse(await(lock0.isLockedByMe()));
      assertFalse(await(lock1.isLockedByMe()));
      assertFalse(await(lock2.isLockedByMe()));

      // lock1 from cm1 acquires the lock
      await(lock1.lock());

      // lock1 from cm1 holds the lock
      assertFalse(await(lock0.isLockedByMe()));
      assertTrue(await(lock1.isLockedByMe()));
      assertFalse(await(lock2.isLockedByMe()));
   }

   @Test
   public void testLockWhenLockIsRemoved() throws Throwable {
      ClusteredLockManager cm0 = clusteredLockManager(0);
      ClusteredLockManager cm1 = clusteredLockManager(1);
      ClusteredLockManager cm2 = clusteredLockManager(2);
      ClusteredLock lock0 = cm0.get(LOCK_NAME);
      ClusteredLock lock1 = cm1.get(LOCK_NAME);
      ClusteredLock lock2 = cm2.get(LOCK_NAME);

      // lock0 from cm0 acquires the lock
      await(lock0.lock());
      CompletableFuture<Void> lock1Request = lock1.lock();
      CompletableFuture<Void> lock2Request = lock2.lock();
      assertFalse(lock1Request.isDone());
      assertFalse(lock2Request.isDone());

      assertTrue(await(cm0.remove(LOCK_NAME)));
      assertNull(await(lock1Request
            .exceptionally(e -> {
               assertException(ClusteredLockException.class, e);
               assertTrue(e.getMessage().contains(Log.LOCK_DELETE_MSG));
               return null;
            })));
      assertNull(await(lock2Request
            .exceptionally(e -> {
               assertException(ClusteredLockException.class, e);
               assertTrue(e.getMessage().contains(Log.LOCK_DELETE_MSG));
               return null;
            })));
   }

   @Test
   public void testTryLockWithTimeoutWithCountersInParallelOnSingleLock() throws Throwable {
      AtomicInteger counter = new AtomicInteger();

      ClusteredLock lock = clusteredLockManager(0).get(LOCK_NAME);

      CompletableFuture<Void> lockRes0 = lock.tryLock(1000, TimeUnit.MILLISECONDS).thenAccept(r -> {
         if (r) {
            counter.incrementAndGet();
            lock.unlock();
         }
      });

      CompletableFuture<Void> lockRes1 = lock.tryLock(1000, TimeUnit.MILLISECONDS).thenAccept(r -> {
         if (r) {
            counter.incrementAndGet();
            lock.unlock();
         }
      });

      CompletableFuture<Void> lockRes2 = lock.tryLock(1000, TimeUnit.MILLISECONDS).thenAccept(r -> {
         if (r) {
            counter.incrementAndGet();
            lock.unlock();
         }
      });

      await(lockRes0);
      await(lockRes1);
      await(lockRes2);

      assertEquals(3, counter.get());
   }

   @Test
   public void testTryLockWithTimeoutWithCountersInParallelOnMultiLocks() throws Throwable {
      AtomicInteger counter = new AtomicInteger();

      ClusteredLock lock0 = clusteredLockManager(0).get(LOCK_NAME);
      ClusteredLock lock1 = clusteredLockManager(1).get(LOCK_NAME);
      ClusteredLock lock2 = clusteredLockManager(2).get(LOCK_NAME);

      CompletableFuture<Void> lockRes0 = lock0.tryLock(1000, TimeUnit.MILLISECONDS).thenAccept(r -> {
         if (r) {
            counter.incrementAndGet();
            lock0.unlock();
         }
      });

      CompletableFuture<Void> lockRes1 = lock1.tryLock(1000, TimeUnit.MILLISECONDS).thenAccept(r -> {
         if (r) {
            counter.incrementAndGet();
            lock1.unlock();
         }
      });

      CompletableFuture<Void> lockRes2 = lock2.tryLock(1000, TimeUnit.MILLISECONDS).thenAccept(r -> {
         if (r) {
            counter.incrementAndGet();
            lock2.unlock();
         }
      });

      await(lockRes0);
      await(lockRes1);
      await(lockRes2);

      assertEquals(3, counter.get());
   }

   @Test
   public void testTryLockWithCountersInParallel() throws Throwable {
      AtomicInteger counter = new AtomicInteger();

      ClusteredLock lock0 = clusteredLockManager(0).get(LOCK_NAME);
      ClusteredLock lock1 = clusteredLockManager(1).get(LOCK_NAME);
      ClusteredLock lock2 = clusteredLockManager(2).get(LOCK_NAME);

      CompletableFuture<Void> lockRes0 = lock0.tryLock()
            .thenCompose(result -> {
               if (result) {
                  new Counter(counter, 1, 100).run();
                  return lock0.unlock();
               }
               return CompletableFuture.completedFuture(null);
            });

      CompletableFuture<Void> lockRes1 = lock1.tryLock()
            .thenCompose(result -> {
               if (result) {
                  new Counter(counter, 1, 100).run();
                  return lock1.unlock();
               }
               return CompletableFuture.completedFuture(null);
            });

      CompletableFuture<Void> lockRes2 = lock2.tryLock()
            .thenCompose(result -> {
               if (result) {
                  new Counter(counter, 1, 100).run();
                  return lock2.unlock();
               }
               return CompletableFuture.completedFuture(null);
            });

      await(lockRes0);
      await(lockRes1);
      await(lockRes2);

      assertEquals(1, counter.get());
   }

   class Counter implements Runnable {
      private AtomicInteger counter;
      private int delta;
      private long millis;

      Counter(AtomicInteger counter, int delta, long millis) {
         this.counter = counter;
         this.delta = delta;
         this.millis = millis;
      }

      @Override
      public void run() {
         // Sleep a while
         try {
            TimeUnit.MILLISECONDS.sleep(millis);
         } catch (InterruptedException e) {
            fail("There was a problem in the Counter");
         }
         counter.addAndGet(delta);
      }
   }
}
