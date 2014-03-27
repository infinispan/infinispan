package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;

/**
 * StoreConfigurationBuilder used for stores/loaders that don't have a configuration builder
 *
 * @author wburns
 * @since 7.0
 */
public class CustomStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<CustomStoreConfiguration, CustomStoreConfigurationBuilder> {
   private Class<?> customStoreClass;

   public CustomStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public CustomStoreConfiguration create() {
      return new CustomStoreConfiguration(customStoreClass, purgeOnStartup, fetchPersistentState, ignoreModifications,
                                          async.create(), singletonStore.create(), preload, shared, properties);
   }

   @Override
   public Builder<?> read(CustomStoreConfiguration template) {
      super.read(template);
      customStoreClass = template.customStoreClass();
      return this;
   }

   @Override
   public CustomStoreConfigurationBuilder self() {
      return this;
   }

   public CustomStoreConfigurationBuilder customStoreClass(Class<?> customStoreClass) {
      this.customStoreClass = customStoreClass;
      return this;
   }
}
