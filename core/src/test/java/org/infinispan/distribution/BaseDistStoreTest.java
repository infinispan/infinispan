package org.infinispan.distribution;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;

/**
 * DistSyncCacheStoreTest.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public abstract class BaseDistStoreTest<K, V> extends BaseDistFunctionalTest<K, V> {
   protected boolean shared;
   protected boolean preload;

   @Override
   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder cfg = super.buildConfiguration();
      if (shared) {

         cfg.persistence().addStore(new DummyInMemoryStoreConfigurationBuilder(cfg.persistence())
                                          .storeName(getClass().getSimpleName())).shared(shared).preload(preload);
      } else {
         cfg.persistence().addStore(new DummyInMemoryStoreConfigurationBuilder(cfg.persistence())).shared(shared).preload(preload);
      }
      return cfg;
   }
}
