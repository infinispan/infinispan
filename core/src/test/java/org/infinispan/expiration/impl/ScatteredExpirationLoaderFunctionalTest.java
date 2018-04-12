package org.infinispan.expiration.impl;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.test.fwk.InTransactionMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Tests to make sure that when expiration occurs it occurs across the cluster when a loader is in use and a scattered
 * cache is in use
 *
 * @author William Burns
 * @since 9.3
 */
@Test(groups = "functional", testName = "expiration.impl.ClusterExpirationLoaderFunctionalTest")
@InCacheMode({CacheMode.SCATTERED_SYNC})
@InTransactionMode({TransactionMode.NON_TRANSACTIONAL})
public class ScatteredExpirationLoaderFunctionalTest extends ClusterExpirationFunctionalTest {
   @Override
   protected void createCluster(ConfigurationBuilder builder, int count) {
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      super.createCluster(builder, count);
   }
}
