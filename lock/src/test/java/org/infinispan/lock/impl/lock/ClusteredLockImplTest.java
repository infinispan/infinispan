package org.infinispan.lock.impl.lock;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.test.Exceptions.assertException;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.lock.BaseClusteredLockTest;
import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockConfiguration;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.exception.ClusteredLockException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "clusteredLock.ClusteredLockImplTest")
public class ClusteredLockImplTest extends BaseClusteredLockTest {

   protected static final String LOCK_NAME = "ClusteredLockImplTest";

   private ClusteredLockManager cm;
   private ClusteredLock lock;

   @Override
   protected int clusterSize() {
      return 2;
   }

   @BeforeClass
   public void createLockManager() throws Throwable {
      cm = clusteredLockManager(0);
   }

   @AfterClass
   public void destroyLock() throws Throwable {
      await(cm.remove(LOCK_NAME));
   }

   @BeforeMethod
   public void createLock() {
      cm.defineLock(LOCK_NAME, new ClusteredLockConfiguration());
      lock = clusteredLockManager(0).get(LOCK_NAME);
      await(lock.unlock());
   }

   @AfterMethod
   public void releaseLock() {
      if (cm.isDefined(LOCK_NAME)) {
         await(lock.unlock());
      }
   }

   public void testLock() throws Throwable {
      assertFalse(await(lock.isLocked()));
      await(lock.lock());
      assertTrue(await(lock.isLocked()));
   }

   public void testTryLock() throws Throwable {
      assertTrue(await(lock.tryLock()));
      assertFalse(await(lock.tryLock()));
   }

   public void testTryLockWithZeroTimeout() throws Throwable {
      assertTrue(await(lock.tryLock(0, TimeUnit.HOURS)));
      assertFalse(await(lock.tryLock(0, TimeUnit.HOURS)));
      assertTrue(await(lock.isLockedByMe()));
      assertTrue(await(lock.isLocked()));
   }

   public void testTryLockWithNegativeTimeout() throws Throwable {
      assertTrue(await(lock.tryLock(-10, TimeUnit.HOURS)));
      assertFalse(await(lock.tryLock(-10, TimeUnit.HOURS)));
      assertTrue(await(lock.isLockedByMe()));
      assertTrue(await(lock.isLocked()));
   }

   public void testFastLockWithTimeout() throws Throwable {
      assertTrue(await(lock.tryLock(1, TimeUnit.NANOSECONDS)));
      assertTrue(await(lock.isLockedByMe()));
      assertTrue(await(lock.isLocked()));
   }

   public void testTryLockWithTimeoutAfterLockWithSmallTimeout() throws Throwable {
      assertTrue(await(lock.tryLock()));
      CompletableFuture<Boolean> tryLock = lock.tryLock(1, TimeUnit.NANOSECONDS);
      await(lock.unlock());
      assertFalse(await(tryLock));
   }

   public void testTryLockWithTimeoutAfterLockWithBigTimeout() throws Throwable {
      assertTrue(await(lock.tryLock()));
      CompletableFuture<Boolean> tryLock = lock.tryLock(1, TimeUnit.SECONDS);
      await(lock.unlock());
      assertTrue(await(tryLock));
   }

   public void testUnlock() throws Throwable {
      assertTrue(await(lock.tryLock()));
      assertFalse(await(lock.tryLock()));
      await(lock.unlock());
      assertTrue(await(lock.tryLock()));
   }

   public void testIsLockedByMe() throws Throwable {
      assertFalse(await(lock.isLockedByMe()));
      await(lock.lock());
      assertTrue(await(lock.isLockedByMe()));
   }

   public void testLockAfterLockRemove() throws Throwable {
      await(cm.remove(LOCK_NAME));

      CompletableFuture<Void> call = lock.lock();
      await(call
            .exceptionally(e -> {
               assertException(ClusteredLockException.class, e);
               //assertEquals(Log.LOCK_DELETE_MSG, e.getMessage());
               return null;
            }));
      assertTrue(call.isCompletedExceptionally());
   }

   public void testTryLockAfterLockRemove() throws Throwable {
      await(cm.remove(LOCK_NAME));

      CompletableFuture<Boolean> call = lock.tryLock();
      await(call
            .exceptionally(e -> {
               assertException(ClusteredLockException.class, e);
               //assertEquals(Log.LOCK_DELETE_MSG, e.getMessage());
               return null;
            }));
      assertTrue(call.isCompletedExceptionally());
   }

   public void testTryLockWithTimeoutAfterLockRemove() throws Throwable {
      await(cm.remove(LOCK_NAME));

      CompletableFuture<Boolean> call = lock.tryLock(100, TimeUnit.MILLISECONDS);
      await(call
            .exceptionally(e -> {
               assertException(ClusteredLockException.class, e);
               //assertEquals(Log.LOCK_DELETE_MSG, e.getMessage());
               return null;
            }));
      assertTrue(call.isCompletedExceptionally());
   }

   public void testIsLockedAfterLockRemove() throws Throwable {
      await(cm.remove(LOCK_NAME));

      CompletableFuture<Boolean> call = lock.isLocked();
      await(call
            .exceptionally(e -> {
               assertException(ClusteredLockException.class, e);
               //assertEquals(Log.LOCK_DELETE_MSG, e.getMessage());
               return null;
            }));
      assertTrue(call.isCompletedExceptionally());
   }

   public void testIsLockedByMeAfterLockRemove() throws Throwable {
      await(cm.remove(LOCK_NAME));

      CompletableFuture<Boolean> call = lock.isLockedByMe();
      await(call
            .exceptionally(e -> {
               assertException(ClusteredLockException.class, e);
               //assertEquals(Log.LOCK_DELETE_MSG, e.getMessage());
               return null;
            }));
      assertTrue(call.isCompletedExceptionally());
   }
}
