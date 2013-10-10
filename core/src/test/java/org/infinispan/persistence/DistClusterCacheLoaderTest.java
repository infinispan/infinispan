package org.infinispan.persistence;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Tester for {@link org.infinispan.persistence.cluster.ClusterLoader} in Replicated caches
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "persistence.DistClusterCacheLoaderTest")
public class DistClusterCacheLoaderTest extends ClusterCacheLoaderTest {

   @Override
   protected CacheMode cacheMode() {
      return CacheMode.DIST_SYNC;
   }
}
