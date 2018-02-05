package org.infinispan.lock;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.TimeUnit;

import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockConfiguration;
import org.infinispan.lock.api.ClusteredLockManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Vert.x-Infinispan cluster manager has some tests where there are just 2 nodes. To avoid failures, we need to add some
 * tests where the cluster is formed by 2 nodes where we will one and the other can acquire and release the lock
 */
@Test(groups = "functional", testName = "clusteredLock.ClusteredLockWith2NodesTest")
public class ClusteredLockWith2NodesTest extends BaseClusteredLockTest {

   protected static final String LOCK_NAME = "ClusteredLockWith2NodesTest";

   @Override
   protected int clusterSize() {
      return 2;
   }

   @BeforeMethod(alwaysRun = true)
   public void createLock() throws Throwable {
      ClusteredLockManager m1 = clusteredLockManager(0);
      m1.defineLock(LOCK_NAME, new ClusteredLockConfiguration());
   }

   @AfterMethod(alwaysRun = true)
   protected void destroyLock() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(1);
      await(clusteredLockManager.remove(LOCK_NAME));
   }

   @Test
   public void testTryLockAndKillLocking() throws Throwable {
      ClusteredLock firstLockOwner = clusteredLockManager(0).get(LOCK_NAME);
      ClusteredLock secondLockOwner = clusteredLockManager(1).get(LOCK_NAME);

      StringBuilder value = new StringBuilder();
      await(firstLockOwner.tryLock().whenComplete((firstTryLockResult, ex1) -> {
         if(ex1 == null) {
            if(firstTryLockResult) killCacheManagers(manager(0));
            else fail("Manager 0 could not acquire the lock");
         } else {
            fail(ex1.getMessage());
         }

         await(secondLockOwner.tryLock(1, TimeUnit.SECONDS).whenComplete((secondTryLockResult, ex2) -> {
            if (ex2 == null && secondTryLockResult) {
               value.append("hello");
            } else {
               fail(ex2.getMessage());
            }
         }));
      }));

      assertEquals("hello", value.toString());
   }

}
