package org.infinispan.lock;

import static java.util.Arrays.asList;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.configuration.Reliability;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "clusteredLock.AvailableReliabilitySplitBrainTest")
public class AvailableReliabilitySplitBrainTest extends BaseClusteredLockSplitBrainTest {

   public AvailableReliabilitySplitBrainTest() {
      super();
      reliability = Reliability.AVAILABLE;
      numOwner = 6;
      cacheMode = CacheMode.DIST_SYNC;
   }

   @Override
   protected String getLockName() {
      return "AvailableReliabilitySplitBrainTest";
   }

   @Test
   public void testLockCreationWhenPartitionHappening() {
      ClusteredLockManager clusteredLockManager = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(0));
      await(clusteredLockManager.remove(getLockName()));

      splitCluster(new int[]{0, 1, 2}, new int[]{3, 4, 5});

      for (EmbeddedCacheManager cm : getCacheManagers()) {
         ClusteredLockManager clm = EmbeddedClusteredLockManagerFactory.from(cm);
         clm.defineLock(getLockName());
      }
   }

   @Test
   public void testLockUseAfterPartitionWithoutMajority() {

      ClusteredLockManager clm0 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(0));
      ClusteredLockManager clm1 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(1));
      ClusteredLockManager clm2 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(2));
      ClusteredLockManager clm3 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(3));
      ClusteredLockManager clm4 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(4));
      ClusteredLockManager clm5 = EmbeddedClusteredLockManagerFactory.from(getCacheManagers().get(5));

      clm0.defineLock(getLockName());
      assertTrue(clm0.isDefined(getLockName()));

      splitCluster(new int[]{0, 1, 2}, new int[]{3, 4, 5});

      partition(0).assertDegradedMode();
      partition(1).assertDegradedMode();

      ClusteredLock lock0 = clm0.get(getLockName());
      ClusteredLock lock1 = clm1.get(getLockName());
      ClusteredLock lock2 = clm2.get(getLockName());
      ClusteredLock lock3 = clm3.get(getLockName());
      ClusteredLock lock4 = clm4.get(getLockName());
      ClusteredLock lock5 = clm5.get(getLockName());

      asList(lock0, lock1, lock2, lock3, lock4, lock5).forEach(lock -> {
         assertNotNull(lock);
         Boolean tryLock = await(lock.tryLock());
         assertTrue(tryLock);
         await(lock.unlock());
      });
   }
}
