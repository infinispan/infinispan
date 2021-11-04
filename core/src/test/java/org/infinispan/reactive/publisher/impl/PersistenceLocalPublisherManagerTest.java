package org.infinispan.reactive.publisher.impl;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "reactive.publisher.impl.PersistenceLocalPublisherManagerTest")
// Scattered cache is disabled due to https://issues.redhat.com/browse/ISPN-13493
@InCacheMode({CacheMode.REPL_SYNC, CacheMode.DIST_SYNC/*, CacheMode.SCATTERED_SYNC*/})
public class PersistenceLocalPublisherManagerTest extends SimpleLocalPublisherManagerTest {

   @Override
   ConfigurationBuilder cacheConfiguration() {
      ConfigurationBuilder configurationBuilder = super.cacheConfiguration();
      configurationBuilder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      configurationBuilder.memory().maxCount(10);
      return configurationBuilder;
   }
}
