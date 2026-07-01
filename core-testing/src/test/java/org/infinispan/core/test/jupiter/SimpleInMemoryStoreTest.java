package org.infinispan.core.test.jupiter;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.CustomStoreConfigurationBuilder;
import org.infinispan.persistence.spi.NonBlockingStore;

/**
 * Validates the {@link AbstractNonBlockingStoreTest} harness using the
 * {@link SimpleInMemoryStore} reference implementation.
 */
class SimpleInMemoryStoreTest extends AbstractNonBlockingStoreTest {

   @Override
   protected NonBlockingStore<Object, Object> createStore() {
      return new SimpleInMemoryStore<>();
   }

   @Override
   protected Configuration buildConfig(ConfigurationBuilder builder) {
      builder.persistence()
            .addStore(CustomStoreConfigurationBuilder.class)
            .customStoreClass(SimpleInMemoryStore.class);
      return builder.build();
   }
}
