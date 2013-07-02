package org.infinispan.distribution;

import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;

/**
 * DistSyncCacheStoreTest.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public abstract class BaseDistCacheStoreTest extends BaseDistFunctionalTest {
   protected boolean shared;
   protected boolean preload;

   @Override
   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder cfg = super.buildConfiguration();
      cfg.loaders().shared(shared);
      if (shared) {
         cfg.loaders().addStore(new DummyInMemoryCacheStoreConfigurationBuilder(cfg.loaders())
               .storeName(getClass().getSimpleName()));
      } else {
         cfg.loaders().addStore(new DummyInMemoryCacheStoreConfigurationBuilder(cfg.loaders()));
      }
      cfg.loaders().preload(preload);
      return cfg;
   }
}
