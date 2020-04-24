package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;

/**
 * @deprecated Since 11.0, {@link MemoryConfiguration} is used to defined the data container memory
 * eviction and sizing.
 */
@Deprecated
public class MemoryStorageConfiguration implements ConfigurationInfo {

   public static final AttributeDefinition<Long> SIZE = AttributeDefinition.builder("size", -1L).build();
   public static final AttributeDefinition<EvictionType> EVICTION_TYPE = AttributeDefinition.builder("type", EvictionType.COUNT).xmlName(org.infinispan.configuration.parsing.Attribute.EVICTION.getLocalName()).build();
   public static final AttributeDefinition<EvictionStrategy> EVICTION_STRATEGY = AttributeDefinition.builder("strategy", EvictionStrategy.NONE).build();
   public static final AttributeDefinition<StorageType> STORAGE_TYPE = AttributeDefinition.builder("storage-type", StorageType.OBJECT).build();

   private final AttributeSet attributes;
   private final boolean enabled;
   private final ElementDefinition<MemoryStorageConfiguration> elementDefinition;

   static public AttributeSet attributeDefinitionSet() {
      return new AttributeSet(MemoryStorageConfiguration.class, SIZE, EVICTION_TYPE, EVICTION_STRATEGY, STORAGE_TYPE);
   }

   public MemoryStorageConfiguration(AttributeSet attributes, boolean enabled) {
      this.attributes = attributes;
      this.enabled = enabled;
      StorageType storageType = attributes.attribute(STORAGE_TYPE).get();
      String storage = storageType == null ? StorageType.OBJECT.getElement().getLocalName() : storageType.getElement().getLocalName();
      this.elementDefinition = new DefaultElementDefinition<>(storage, true, false);
   }

   /**
    * @return true if the {@link MemoryStorageConfigurationBuilder} was non-empty when building this configuration.
    */
   boolean isEnabled() {
      return enabled;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition<MemoryStorageConfiguration> getElementDefinition() {
      return elementDefinition;
   }

   public StorageType storageType() {
      return attributes.attribute(STORAGE_TYPE).get();
   }

   public long size() {
      return attributes.attribute(SIZE).get();
   }

   public EvictionType evictionType() {
      return attributes.attribute(EVICTION_TYPE).get();
   }

   public EvictionStrategy evictionStrategy() {
      return attributes.attribute(EVICTION_STRATEGY).get();
   }

   public void size(long newSize) {
      attributes.attribute(SIZE).set(newSize);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MemoryStorageConfiguration that = (MemoryStorageConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "MemoryStorageConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
