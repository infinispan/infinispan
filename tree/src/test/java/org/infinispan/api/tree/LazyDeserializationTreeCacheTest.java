package org.infinispan.api.tree;

import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.tree.TreeCache;
import org.infinispan.tree.TreeCacheImpl;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "api.tree.LazyDeserializationTreeCacheTest")
public class LazyDeserializationTreeCacheTest extends SingleCacheManagerTest {

   TreeCache cache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      Configuration c = getDefaultStandaloneConfig(true);
      c.setInvocationBatchingEnabled(true);
      c.setUseLazyDeserialization(true);
      return TestCacheManagerFactory.createCacheManager(c, true);
   }

   public void testStartTreeCache() {
      cache = new TreeCacheImpl(cacheManager.getCache());
   }

}
