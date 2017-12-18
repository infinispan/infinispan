package org.infinispan.lock;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.test.Exceptions.assertException;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.exception.ClusteredLockException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "clusteredLock.ClusteredLockSplitBrainTest")
public class ClusteredLockSplitBrainTest extends BasePartitionHandlingTest {

   private static final String LOCK_NAME = "ClusteredLockSplitBrainTest";

   @Test
   public void testLockCreationWhenPartitionHappening() throws Throwable {
      ClusteredLockManager clusteredLockManager = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(0));
      await(clusteredLockManager.remove(LOCK_NAME));

      splitCluster(new int[]{0, 1}, new int[]{2, 3});

      eventuallyEquals(2, () -> advancedCache(0).getRpcManager().getTransport().getMembers().size());
      eventuallyEquals(2, () -> advancedCache(1).getRpcManager().getTransport().getMembers().size());
      eventuallyEquals(2, () -> advancedCache(2).getRpcManager().getTransport().getMembers().size());
      eventuallyEquals(2, () -> advancedCache(3).getRpcManager().getTransport().getMembers().size());

      for (EmbeddedCacheManager cm : getCacheManagers()) {
         ClusteredLockManager clm = EmbeddedClusteredLockManagerFactory.from(cm);
         eventually(() -> availabilityExceptionRaised(clm));
      }
   }

   private boolean availabilityExceptionRaised(ClusteredLockManager clm) {
      Exception ex = null;
      try {
         clm.defineLock(LOCK_NAME);
      } catch (AvailabilityException a) {
         ex = a;
      }
      return ex != null;
   }

   @Test
   public void testLockUseAfterPartitionWithoutMajority() throws Throwable {

      ClusteredLockManager clm0 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(0));
      ClusteredLockManager clm1 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(1));
      ClusteredLockManager clm2 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(2));
      ClusteredLockManager clm3 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(3));

      clm0.defineLock(LOCK_NAME);

      assertTrue(clm0.isDefined(LOCK_NAME));
      ClusteredLock lock0 = clm0.get(LOCK_NAME);
      assertNotNull(lock0);

      ClusteredLock lock1 = clm1.get(LOCK_NAME);
      assertNotNull(lock1);

      ClusteredLock lock2 = clm2.get(LOCK_NAME);
      assertNotNull(lock2);

      ClusteredLock lock3 = clm3.get(LOCK_NAME);
      assertNotNull(lock3);

      splitCluster(new int[]{0, 1}, new int[]{2, 3});

      assertNull(await(lock0.tryLock(5, TimeUnit.SECONDS).exceptionally(ex -> {
         assertException(ClusteredLockException.class, ex);
         assertException(AvailabilityException.class, ex.getCause());
         return null;
      })));
      assertNull(await(lock1.tryLock(5, TimeUnit.SECONDS).exceptionally(ex -> {
         assertException(ClusteredLockException.class, ex);
         assertException(AvailabilityException.class, ex.getCause());
         return null;
      })));
      assertNull(await(lock2.tryLock(5, TimeUnit.SECONDS).exceptionally(ex -> {
         assertException(ClusteredLockException.class, ex);
         assertException(AvailabilityException.class, ex.getCause());
         return null;
      })));
      assertNull(await(lock3.tryLock(5, TimeUnit.SECONDS).exceptionally(ex -> {
         assertException(ClusteredLockException.class, ex);
         assertException(AvailabilityException.class, ex.getCause());
         return null;
      })));
   }

   @Test
   public void testLockUseAfterWithMajorityPartition() throws Throwable {

      ClusteredLockManager clm0 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(0));
      ClusteredLockManager clm1 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(1));
      ClusteredLockManager clm2 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(2));
      ClusteredLockManager clm3 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(3));

      assertTrue(clm0.defineLock(LOCK_NAME));
      assertFalse(clm1.defineLock(LOCK_NAME));
      assertFalse(clm2.defineLock(LOCK_NAME));
      assertFalse(clm3.defineLock(LOCK_NAME));

      ClusteredLock lock0 = clm0.get(LOCK_NAME);
      ClusteredLock lock1 = clm1.get(LOCK_NAME);
      ClusteredLock lock2 = clm2.get(LOCK_NAME);
      ClusteredLock lock3 = clm3.get(LOCK_NAME);

      splitCluster(new int[]{0, 1, 2}, new int[]{3});

      await(lock0.tryLock(5, TimeUnit.SECONDS)
            .thenRun(() -> {
               lock0.unlock();
            }).exceptionally(ex -> {
               fail("Should not be failing from majority partition");
               return null;
            }));

      await(lock1.tryLock(5, TimeUnit.SECONDS)
            .thenRun(() -> {
               lock1.unlock();
            }).exceptionally(ex -> {
               fail("Should not be failing from majority partition");
               return null;
            }));

      await(lock2.tryLock(5, TimeUnit.SECONDS)
            .thenRun(() -> {
               lock2.unlock();
            }).exceptionally(ex -> {
               fail("Should not be failing from majority partition");
               return null;
            }));

      assertFailureFromMinorityPartition(lock3);
   }

   @Test
   public void testAutoReleaseIfLockIsAcquiredFromAMinorityPartition() throws Throwable {

      ClusteredLockManager clm0 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(0));
      ClusteredLockManager clm1 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(1));

      assertTrue(clm0.defineLock(LOCK_NAME));

      ClusteredLock lock0 = clm0.get(LOCK_NAME);
      ClusteredLock lock1 = clm1.get(LOCK_NAME);

      await(lock0.tryLock());
      assertTrue(await(lock0.isLockedByMe()));

      splitCluster(new int[]{0}, new int[]{1, 2, 3});

      assertFailureFromMinorityPartition(lock0);
      assertTrue(await(lock1.tryLock(1, TimeUnit.SECONDS)));
   }

   private void assertFailureFromMinorityPartition(ClusteredLock lock3) {
      await(lock3.lock()
            .thenRun(() -> {
               fail("Should fail from minority partition");
               lock3.unlock();
            }).exceptionally(ex -> {
               assertException(CompletionException.class, ex);
               assertException(ClusteredLockException.class, ex.getCause());
               assertException(AvailabilityException.class, ex.getCause().getCause());
               return null;
            }));
   }
}
