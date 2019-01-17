package org.infinispan.lock;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lock.exception.ClusteredLockException;
import org.infinispan.manager.EmbeddedCacheManagerStartupException;
import org.infinispan.test.Exceptions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "clusteredLock.ClusteredLockWithZeroCapacityIncorrectConfigTest")
public class ClusteredLockWithZeroCapacityIncorrectConfigTest extends BaseClusteredLockTest {

   public ClusteredLockWithZeroCapacityIncorrectConfigTest() {
      super();
      numOwner = -1;
      cacheMode = CacheMode.DIST_SYNC;
   }

   @Override
   protected int clusterSize() {
      return 3;
   }

   @BeforeClass(alwaysRun = true)
   @Override
   public void createBeforeClass() throws Throwable {
      // Nothing
   }

   @Override
   protected GlobalConfigurationBuilder configure(int nodeId) {
      return super.configure(nodeId).zeroCapacityNode(nodeId == 1);
   }

   public void testClusterStartupError() {
      Exceptions.expectException(RuntimeException.class,
            EmbeddedCacheManagerStartupException.class,
            ClusteredLockException.class, this::createCluster);
   }

   private void createCluster() {
      try {
         createCacheManagers();
      } catch (Throwable throwable) {
         throw new RuntimeException(throwable);
      }
   }
}
