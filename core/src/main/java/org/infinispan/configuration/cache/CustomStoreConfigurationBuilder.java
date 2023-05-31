package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.CustomStoreConfiguration.CUSTOM_STORE_CLASS;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;

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
      return new CustomStoreConfiguration(attributes.protect(), async.create());
   }

   public CustomStoreConfigurationBuilder customStoreClass(Class<?> customStoreClass) {
      attributes.attribute(CUSTOM_STORE_CLASS).set(customStoreClass);
      return this;
   }

   @Override
   public Builder<?> read(CustomStoreConfiguration template, Combine combine) {
      super.read(template, combine);
      return this;
   }

   @Override
   public CustomStoreConfigurationBuilder self() {
      return this;
   }

}
