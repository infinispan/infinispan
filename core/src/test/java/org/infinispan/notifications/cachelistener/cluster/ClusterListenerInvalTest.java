package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * This tests is to make sure that a cluster listener may not be used with invalidation.  The reason being is that
 * we don't have a single owner to guarantee ordering.
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.cluster.ClusterListenerInvalTest")
public class ClusterListenerInvalTest extends SingleCacheManagerTest {
   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.INVALIDATION_SYNC);
      return TestCacheManagerFactory.createClusteredCacheManager(c);
   }

   @Listener(clustered = true)
   private static class ClusterListener {
      @CacheEntryCreated
      public void created(CacheEntryEvent event) {

      }
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testEnsureClusterListenerNotSupportedForInvalidation() {
      cache.addListener(new ClusterListener());
   }
}
