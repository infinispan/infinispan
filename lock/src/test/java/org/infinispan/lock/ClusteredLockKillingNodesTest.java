package org.infinispan.lock;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockConfiguration;
import org.infinispan.lock.api.ClusteredLockManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "clusteredLock.ClusteredLockKillingNodesTest")
public class ClusteredLockKillingNodesTest extends BaseClusteredLockTest {

   protected static final String LOCK_NAME = "ClusteredLockKillingNodesTest";

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
   public void testLockWithAcquisitionAndKill() throws Throwable {
      ClusteredLock firstLockOwner = clusteredLockManager(1).get(LOCK_NAME);
      ClusteredLock secondLockOwner = clusteredLockManager(2).get(LOCK_NAME);

      StringBuilder value = new StringBuilder();
      await(firstLockOwner.lock().thenRun(() -> {
         killCacheManagers(manager(1));
         await(secondLockOwner.lock().thenRun(() -> value.append("hello")));
      }));

      assertEquals(value.toString(), "hello");
   }

}
