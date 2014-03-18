package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class CustomCacheWriterConfigurationBuilder extends AbstractStoreConfigurationBuilder<CustomCacheWriterConfiguration, CustomCacheWriterConfigurationBuilder> {


   private String someProperty;
   private String location;

   public CustomCacheWriterConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public CustomCacheWriterConfiguration create() {
      return new CustomCacheWriterConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(),
                                                singletonStore.create(), preload, shared, properties, someProperty);
   }

   @Override
   public CustomCacheWriterConfigurationBuilder self() {
      return this;
   }

   public CustomCacheWriterConfigurationBuilder someProperty(String some) {
      this.someProperty = some;
      return this;
   }

   public CustomCacheWriterConfigurationBuilder location(String some) {
      this.location = some;
      return this;
   }

}

