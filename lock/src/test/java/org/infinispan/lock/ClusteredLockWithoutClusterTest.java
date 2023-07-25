package org.infinispan.lock;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jupiter.SingleCacheManagerTest;
import org.infinispan.jupiter.TestTags;
import org.infinispan.lock.exception.ClusteredLockException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.SMOKE)
public class ClusteredLockWithoutClusterTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(false);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(false);
      cm.defineConfiguration("test", c.build());
      cache = cm.getCache("test");
      return cm;
   }

   @Test
   public void testNeedsCluster() {
      Exceptions.expectException(ClusteredLockException.class, () -> EmbeddedClusteredLockManagerFactory.from(cacheManager));
   }
}
