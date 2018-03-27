package org.infinispan.lock;

import static java.util.Arrays.asList;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.lock.impl.ClusteredLockModuleLifecycle.CLUSTERED_LOCK_CACHE_NAME;
import static org.infinispan.test.Exceptions.assertException;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.exception.ClusteredLockException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "clusteredLock.ClusteredLockSplitBrainTest")
public class ClusteredLockSplitBrainTest extends BasePartitionHandlingTest {

   private static final String LOCK_NAME = "ClusteredLockSplitBrainTest";

   public ClusteredLockSplitBrainTest() {
      this.numMembersInCluster = 6;
      this.cacheMode = null;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = cacheConfiguration();
      dcc.clustering().cacheMode(CacheMode.REPL_SYNC).partitionHandling().whenSplit(partitionHandling);
      createClusteredCaches(numMembersInCluster, dcc, new TransportFlags().withFD(true).withMerge(true));
      waitForClusterToForm(CLUSTERED_LOCK_CACHE_NAME);
   }

   @Test
   public void testLockCreationWhenPartitionHappening() throws Throwable {
      ClusteredLockManager clusteredLockManager = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(0));
      await(clusteredLockManager.remove(LOCK_NAME));

      splitCluster(new int[]{0, 1, 2}, new int[]{3, 4, 5});

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
      ClusteredLockManager clm4 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(4));
      ClusteredLockManager clm5 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(5));

      clm0.defineLock(LOCK_NAME);
      assertTrue(clm0.isDefined(LOCK_NAME));

      ClusteredLock lock0 = clm0.get(LOCK_NAME);
      ClusteredLock lock1 = clm1.get(LOCK_NAME);
      ClusteredLock lock2 = clm2.get(LOCK_NAME);
      ClusteredLock lock3 = clm3.get(LOCK_NAME);
      ClusteredLock lock4 = clm4.get(LOCK_NAME);
      ClusteredLock lock5 = clm5.get(LOCK_NAME);

      splitCluster(new int[]{0, 1, 2}, new int[]{3, 4, 5});

      // Wait for degraded topologies to work around ISPN-9008
      partition(0).assertDegradedMode();
      partition(1).assertDegradedMode();

      asList(lock0, lock1, lock2, lock3, lock4, lock5).forEach(lock -> {
         assertNotNull(lock);
         await(lock.tryLock().whenComplete((r, ex) -> {
            fail("should go the exceptionally! result=" + r + " exception= " + ex);
         }).exceptionally(t -> {
            assertException(CompletionException.class, ClusteredLockException.class, AvailabilityException.class, t);
            return null;
         }));
      });
   }

   @Test
   public void testLockUseAfterPartitionWithMajority() throws Throwable {

      ClusteredLockManager clm0 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(0));
      ClusteredLockManager clm1 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(1));
      ClusteredLockManager clm2 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(2));
      ClusteredLockManager clm3 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(3));
      ClusteredLockManager clm4 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(4));
      ClusteredLockManager clm5 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(5));

      assertTrue(clm0.defineLock(LOCK_NAME));
      assertFalse(clm1.defineLock(LOCK_NAME));
      assertFalse(clm2.defineLock(LOCK_NAME));
      assertFalse(clm3.defineLock(LOCK_NAME));
      assertFalse(clm4.defineLock(LOCK_NAME));
      assertFalse(clm5.defineLock(LOCK_NAME));

      ClusteredLock lock0 = clm0.get(LOCK_NAME);
      ClusteredLock lock1 = clm1.get(LOCK_NAME);
      ClusteredLock lock2 = clm2.get(LOCK_NAME);
      ClusteredLock lock3 = clm3.get(LOCK_NAME);
      ClusteredLock lock4 = clm4.get(LOCK_NAME);
      ClusteredLock lock5 = clm5.get(LOCK_NAME);

      splitCluster(new int[]{0, 1, 2, 3}, new int[]{4, 5});

      asList(lock0, lock1, lock2, lock3).forEach(lock -> {
         assertTryLock(lock);

      });

      assertFailureFromMinorityPartition(lock4);
      assertFailureFromMinorityPartition(lock5);
   }

   @Test
   public void testAutoReleaseIfLockIsAcquiredFromAMinorityPartition() throws Throwable {

      ClusteredLockManager clm0 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(0));
      ClusteredLockManager clm1 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(1));
      ClusteredLockManager clm2 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(2));

      assertTrue(clm0.defineLock(LOCK_NAME));

      ClusteredLock lock0 = clm0.get(LOCK_NAME);
      ClusteredLock lock1 = clm1.get(LOCK_NAME);
      ClusteredLock lock2 = clm2.get(LOCK_NAME);

      await(lock0.tryLock());
      assertTrue(await(lock0.isLockedByMe()));

      splitCluster(new int[]{0}, new int[]{1, 2, 3, 4, 5});

      CompletableFuture<Boolean> tryLock1Result = lock1.tryLock(1, TimeUnit.SECONDS);
      CompletableFuture<Boolean> tryLock2Result = lock2.tryLock(1, TimeUnit.SECONDS);

      assertTrue("Just one of the locks has to work", await(tryLock1Result) ^ await(tryLock2Result));

      assertFailureFromMinorityPartition(lock0);
   }

   @Test
   public void testTryLocksBeforeSplitBrain() throws Throwable {

      ClusteredLockManager clm0 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(0));
      ClusteredLockManager clm1 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(1));
      ClusteredLockManager clm2 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(2));

      assertTrue(clm0.defineLock(LOCK_NAME));

      ClusteredLock lock0 = clm0.get(LOCK_NAME);
      ClusteredLock lock1 = clm1.get(LOCK_NAME);
      ClusteredLock lock2 = clm2.get(LOCK_NAME);

      CompletableFuture<Boolean> tryLock1 = lock1.tryLock();
      CompletableFuture<Boolean> tryLock2 = lock2.tryLock();

      splitCluster(new int[]{0}, new int[]{1, 2, 3, 4, 5});

      assertTrue("Just one of the locks has to work", await(tryLock1) ^ await(tryLock2));

      assertFailureFromMinorityPartition(lock0);
   }

   private void assertTryLock(ClusteredLock lock) {
      assertTrue("Lock acquisition should be true " + lock, await(lock.tryLock(29, TimeUnit.SECONDS)
            .thenApply(tryLockRequest -> {
               if (tryLockRequest) {
                  await(lock.unlock());
               }
               return tryLockRequest;
            }).exceptionally(ex -> {
               fail("Should not be failing from majority partition " + lock + " " + ex.getMessage());
               return Boolean.FALSE;
            })));
   }

   private void assertFailureFromMinorityPartition(ClusteredLock lock) {
      await(lock.tryLock()
            .whenComplete((r, ex) -> {
               fail("Should fail from minority partition");
            }).exceptionally(ex -> {
               assertException(CompletionException.class, ClusteredLockException.class, AvailabilityException.class, ex);
               return null;
            }));
   }
}
