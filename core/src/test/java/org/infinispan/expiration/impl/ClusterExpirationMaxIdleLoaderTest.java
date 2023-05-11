package org.infinispan.expiration.impl;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.expiration.TouchMode;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests to make sure that when max-idle expiration occurs it occurs across the cluster even if a loader is in use
 *
 * @author William Burns
 * @since 8.0
 */
@Test(groups = "functional", testName = "expiration.impl.ClusterExpirationMaxIdleLoaderTest")
public class ClusterExpirationMaxIdleLoaderTest extends ClusterExpirationMaxIdleTest {
   @Override
   protected void createCluster(ConfigurationBuilder builder, int count) {
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      super.createCluster(builder, count);
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new ClusterExpirationMaxIdleLoaderTest().cacheMode(CacheMode.DIST_SYNC).transactional(true),
            new ClusterExpirationMaxIdleLoaderTest().cacheMode(CacheMode.DIST_SYNC).transactional(false),
            new ClusterExpirationMaxIdleLoaderTest().cacheMode(CacheMode.REPL_SYNC).transactional(true),
            new ClusterExpirationMaxIdleLoaderTest().cacheMode(CacheMode.REPL_SYNC).transactional(false),
            new ClusterExpirationMaxIdleLoaderTest().touch(TouchMode.ASYNC).cacheMode(CacheMode.DIST_SYNC).transactional(false),
            new ClusterExpirationMaxIdleLoaderTest().touch(TouchMode.ASYNC).cacheMode(CacheMode.REPL_SYNC).transactional(false),
      };
   }
}
