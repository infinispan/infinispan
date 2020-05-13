package org.infinispan.persistence.dummy;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseNonBlockingStoreTest;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.dummy.DummyInMemoryStoreTest")
public class DummyInMemoryStoreTest extends BaseNonBlockingStoreTest {

   @Override
   protected NonBlockingStore createStore() {
      return new DummyInMemoryStore();
   }

   @Override
   protected Configuration buildConfig(ConfigurationBuilder configurationBuilder) {
      return configurationBuilder.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(getClass().getName())
            .build();
   }
}
