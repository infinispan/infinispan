package org.infinispan.lock;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.TimeUnit;

import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockConfiguration;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * Vert.x-Infinispan cluster manager has some tests where there are just 2 nodes. To avoid failures, we need to add some
 * tests where the cluster is formed by 2 nodes where we will one and the other can acquire and release the lock
 */
@CleanupAfterMethod
@Test(groups = "functional", testName = "clusteredLock.ClusteredLockWith2NodesTest")
public class ClusteredLockWith2NodesTest extends BaseClusteredLockTest {

   private static final String LOCK_NAME = "ClusteredLockWith2NodesTest";

   @Override
   protected int clusterSize() {
      return 2;
   }

   public void testTryLockAndKillCoordinator() {
      doTest(0, 1);
   }

   @Test
   public void testTryLockAndKillNode() {
      doTest(1, 0);
   }

   private void doTest(int killedNode, int survivingNode) {
      ClusteredLockManager m1 = clusteredLockManager(0);
      m1.defineLock(LOCK_NAME, new ClusteredLockConfiguration());

      ClusteredLock firstLockOwner = clusteredLockManager(killedNode).get(LOCK_NAME);
      ClusteredLock secondLockOwner = clusteredLockManager(survivingNode).get(LOCK_NAME);

      Boolean acquired = await(firstLockOwner.tryLock());
      if (!acquired) {
         fail("Manager 0 could not acquire the lock");
      }

      try {
         killCacheManagers(manager(killedNode));

         await(secondLockOwner.tryLock(1, TimeUnit.SECONDS));
      } finally {
         ClusteredLockManager clusteredLockManager = clusteredLockManager(survivingNode);
         await(clusteredLockManager.remove(LOCK_NAME));
      }
   }
}
