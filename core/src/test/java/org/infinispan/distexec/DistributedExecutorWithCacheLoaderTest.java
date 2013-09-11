package org.infinispan.distexec;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests are added for testing DistributedExecutors with stores.
 */
@Test(groups = "functional", testName = "distexec.DistributedExecutorWithCacheLoaderTest")
public class DistributedExecutorWithCacheLoaderTest extends DistributedExecutorTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), false);
      builder.eviction().maxEntries(1).strategy(EvictionStrategy.LRU);
      builder.persistence().passivation(true).addStore(DummyInMemoryStoreConfigurationBuilder.class).storeName(getClass().getSimpleName());
      builder.storeAsBinary().enable();

      createClusteredCaches(2, cacheName(), builder);
   }
}