package org.infinispan.lock;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.TimeUnit;

import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockConfiguration;
import org.infinispan.lock.api.ClusteredLockManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "clusteredLock.ClusteredLockWith2NodesKillingOneTest")
public class ClusteredLockWith2NodesKillingOneTest extends BaseClusteredLockTest {

   protected static final String LOCK_NAME = "ClusteredLockWith2NodesKillingOneTest";

   @Override
   protected int clusterSize() {
      return 2;
   }

   @BeforeClass(alwaysRun = true)
   public void createLock() throws Throwable {
      ClusteredLockManager m1 = clusteredLockManager(0);
      m1.defineLock(LOCK_NAME, new ClusteredLockConfiguration());
   }

   @AfterClass(alwaysRun = true)
   protected void destroyLock() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(1);
      await(clusteredLockManager.remove(LOCK_NAME));
   }

   @Test
   public void testLockWithAcquisitionAndKill() throws Throwable {
      ClusteredLock firstLockOwner = clusteredLockManager(0).get(LOCK_NAME);
      ClusteredLock secondLockOwner = clusteredLockManager(1).get(LOCK_NAME);

      StringBuilder value = new StringBuilder();
      await(firstLockOwner.tryLock(1, TimeUnit.SECONDS).thenAccept(r1 -> {
         if (r1) {
            killCacheManagers(manager(0));
            await(secondLockOwner.tryLock(1, TimeUnit.SECONDS).thenAccept(r2 -> {
               if (r2) value.append("hello");
            }));
         } else {
            fail("Lock should be acquired");
         }
      }));

      assertEquals(value.toString(), "hello");
   }

}
