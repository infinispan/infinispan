package org.infinispan.configuration.global;

import java.util.function.Supplier;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.globalstate.LocalConfigurationStorage;


/**
 * @since 10.0
 */
@BuiltBy(GlobalStorageConfigurationBuilder.class)
public class GlobalStorageConfiguration extends ConfigurationElement<GlobalStorageConfiguration> {
   static final AttributeDefinition<Supplier<? extends LocalConfigurationStorage>> CONFIGURATION_STORAGE_SUPPLIER = AttributeDefinition
         .supplierBuilder("class", LocalConfigurationStorage.class).autoPersist(false)
         .immutable().build();
   static final AttributeDefinition<ConfigurationStorage> STORAGE = AttributeDefinition.builder("storage", ConfigurationStorage.VOLATILE).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalStorageConfiguration.class, CONFIGURATION_STORAGE_SUPPLIER, STORAGE);
   }

   GlobalStorageConfiguration(AttributeSet attributeSet) {
      super("configuration-storage", attributeSet);
   }


   public ConfigurationStorage configurationStorage() {
      return attributes.attribute(STORAGE).get();
   }

   Supplier<? extends LocalConfigurationStorage> storageSupplier() {
      return attributes.attribute(CONFIGURATION_STORAGE_SUPPLIER).get();
   }
}
