package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class CustomCacheLoaderConfigurationBuilder extends AbstractStoreConfigurationBuilder<CustomCacheLoaderConfiguration, CustomCacheLoaderConfigurationBuilder> {


   private String location;

   public CustomCacheLoaderConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public CustomCacheLoaderConfiguration create() {
      return new CustomCacheLoaderConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(),
                                                singletonStore.create(), preload, shared, properties, location);
   }

   @Override
   public Builder<?> read(CustomCacheLoaderConfiguration template) {
      // AbstractStore-specific configuration
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      async.read(template.async());
      singletonStore.read(template.singletonStore());
      preload = template.preload();
      shared = template.shared();

      return this;

   }

   @Override
   public CustomCacheLoaderConfigurationBuilder self() {
      return this;
   }

   public CustomCacheLoaderConfigurationBuilder location(String some) {
      this.location = some;
      return this;
   }

}
