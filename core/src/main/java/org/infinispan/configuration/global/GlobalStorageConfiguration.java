package org.infinispan.configuration.global;

import java.util.function.Supplier;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.globalstate.LocalConfigurationStorage;


/**
 * @since 10.0
 */
public class GlobalStorageConfiguration {
   static final AttributeDefinition<Supplier<? extends LocalConfigurationStorage>> CONFIGURATION_STORAGE_SUPPLIER = AttributeDefinition
         .supplierBuilder("class", LocalConfigurationStorage.class).autoPersist(false)
         .immutable().build();

   private final ConfigurationStorage storage;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalStorageConfiguration.class, CONFIGURATION_STORAGE_SUPPLIER);
   }

   private final AttributeSet attributes;

   GlobalStorageConfiguration(AttributeSet attributeSet, ConfigurationStorage storage) {
      this.attributes = attributeSet;
      this.storage = storage;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public ConfigurationStorage configurationStorage() {
      return storage;
   }

   Supplier<? extends LocalConfigurationStorage> storageSupplier() {
      return attributes.attribute(CONFIGURATION_STORAGE_SUPPLIER).get();
   }
}
