package org.infinispan.distexec;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
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
      builder.memory().storageType(StorageType.BINARY).size(1);
      builder.persistence().passivation(true).addStore(DummyInMemoryStoreConfigurationBuilder.class).storeName(getClass().getSimpleName());

      createClusteredCaches(2, cacheName(), builder);
   }
}
