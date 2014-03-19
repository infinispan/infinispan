package org.infinispan.api.tree;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.tree.TreeCache;
import org.infinispan.tree.impl.TreeCacheImpl;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.tree.LazyDeserializationTreeCacheTest")
public class LazyDeserializationTreeCacheTest extends SingleCacheManagerTest {

   TreeCache cache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      ConfigurationBuilder cb = getDefaultStandaloneCacheConfig(true);
      cb.invocationBatching().enable()
            .storeAsBinary().enable();
      return TestCacheManagerFactory.createCacheManager(cb);
   }

   public void testStartTreeCache() {
      cache = new TreeCacheImpl(cacheManager.getCache());
   }

}
