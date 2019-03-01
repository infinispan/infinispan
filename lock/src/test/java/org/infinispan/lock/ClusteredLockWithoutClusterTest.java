package org.infinispan.lock;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lock.exception.ClusteredLockException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.Exceptions;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "clusteredLock.ClusteredLockWithoutClusterTest")
public class ClusteredLockWithoutClusterTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(false);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(false);
      cm.defineConfiguration("test", c.build());
      cache = cm.getCache("test");
      return cm;
   }

   public void testNeedsCluster() {
      Exceptions.expectException(ClusteredLockException.class, () -> EmbeddedClusteredLockManagerFactory.from(cacheManager));
   }
}
