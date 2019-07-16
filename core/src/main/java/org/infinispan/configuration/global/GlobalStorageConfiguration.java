package org.infinispan.configuration.global;

import java.util.function.Supplier;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.globalstate.LocalConfigurationStorage;


/**
 * @since 10.0
 */
public class GlobalStorageConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<Supplier<? extends LocalConfigurationStorage>> CONFIGURATION_STORAGE_SUPPLIER = AttributeDefinition
         .supplierBuilder("class", LocalConfigurationStorage.class).autoPersist(false)
         .serializer(new AttributeSerializer<Supplier<? extends LocalConfigurationStorage>, ConfigurationInfo, ConfigurationBuilderInfo>() {
            @Override
            public Object getSerializationValue(Attribute<Supplier<? extends LocalConfigurationStorage>> attribute, ConfigurationInfo configurationElement) {
               Supplier<? extends LocalConfigurationStorage> supplier = attribute.get();
               if (supplier == null) return null;
               return supplier.get().getClass().getName();
            }
         })
         .immutable().build();

   private final ElementDefinition elementDefinition;
   private final ConfigurationStorage storage;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalStorageConfiguration.class, CONFIGURATION_STORAGE_SUPPLIER);
   }

   private final AttributeSet attributes;

   GlobalStorageConfiguration(AttributeSet attributeSet, ConfigurationStorage storage) {
      this.attributes = attributeSet;
      this.storage = storage;
      this.elementDefinition = getElementDefinition(storage);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public ConfigurationStorage configurationStorage() {
      return storage;
   }

   Supplier<? extends LocalConfigurationStorage> storageSupplier() {
      return attributes.attribute(CONFIGURATION_STORAGE_SUPPLIER).get();
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return elementDefinition;
   }

   private ElementDefinition getElementDefinition(ConfigurationStorage configurationStorage) {
      switch (configurationStorage) {
         case IMMUTABLE:
            return new DefaultElementDefinition(Element.IMMUTABLE_CONFIGURATION_STORAGE.getLocalName(), true, false);
         case VOLATILE:
            return new DefaultElementDefinition(Element.VOLATILE_CONFIGURATION_STORAGE.getLocalName(), true, false);
         case OVERLAY:
            return new DefaultElementDefinition(Element.OVERLAY_CONFIGURATION_STORAGE.getLocalName(), true, false);
         case MANAGED:
            return new DefaultElementDefinition(Element.MANAGED_CONFIGURATION_STORAGE.getLocalName(), true, false);
         case CUSTOM:
            return new DefaultElementDefinition(Element.CUSTOM_CONFIGURATION_STORAGE.getLocalName(), true, false);
      }
      return null;
   }
}
