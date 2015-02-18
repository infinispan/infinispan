package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.CustomStoreConfiguration.CUSTOM_STORE_CLASS;

import org.infinispan.commons.configuration.Builder;

/**
 * StoreConfigurationBuilder used for stores/loaders that don't have a configuration builder
 *
 * @author wburns
 * @since 7.0
 */
public class CustomStoreConfigurationBuilder extends
      AbstractStoreConfigurationBuilder<CustomStoreConfiguration, CustomStoreConfigurationBuilder> {

   public CustomStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, CustomStoreConfiguration.attributeDefinitionSet());
   }

   @Override
   public CustomStoreConfiguration create() {
      return new CustomStoreConfiguration(attributes.protect(), async.create(), singletonStore.create());
   }

   public CustomStoreConfigurationBuilder customStoreClass(Class<?> customStoreClass) {
      attributes.attribute(CUSTOM_STORE_CLASS).set(customStoreClass);
      return this;
   }

   @Override
   public Builder<?> read(CustomStoreConfiguration template) {
      super.read(template);
      return this;
   }

   @Override
   public CustomStoreConfigurationBuilder self() {
      return this;
   }

}
