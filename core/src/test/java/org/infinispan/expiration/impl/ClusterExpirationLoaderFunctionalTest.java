package org.infinispan.expiration.impl;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.Test;

/**
 * Tests to make sure that when expiration occurs it occurs across the cluster when a loader is in use
 *
 * @author William Burns
 * @since 8.0
 */
@Test(groups = "functional", testName = "expiration.impl.ClusterExpirationLoaderFunctionalTest")
@InCacheMode({CacheMode.DIST_SYNC, CacheMode.REPL_SYNC, CacheMode.SCATTERED_SYNC})
public class ClusterExpirationLoaderFunctionalTest extends ClusterExpirationFunctionalTest {
   @Override
   protected void createCluster(ConfigurationBuilder builder, int count) {
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      super.createCluster(builder, count);
   }
}
