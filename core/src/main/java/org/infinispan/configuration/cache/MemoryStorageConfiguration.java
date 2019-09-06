package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;

public class MemoryStorageConfiguration implements ConfigurationInfo {

   public static final AttributeDefinition<Long> SIZE = AttributeDefinition.builder("size", -1L).build();
   public static final AttributeDefinition<EvictionType> EVICTION_TYPE = AttributeDefinition.builder("type", EvictionType.COUNT).xmlName(org.infinispan.configuration.parsing.Attribute.EVICTION.getLocalName()).build();
   public static final AttributeDefinition<EvictionStrategy> EVICTION_STRATEGY = AttributeDefinition.builder("strategy", EvictionStrategy.NONE).build();
   public static final AttributeDefinition<Integer> ADDRESS_COUNT = AttributeDefinition.builder("address-count", 1_048_576).build();

   private final AttributeSet attributes;
   private final StorageType storageType;
   private final ElementDefinition elementDefinition;

   static public AttributeSet attributeDefinitionSet() {
      return new AttributeSet(MemoryStorageConfiguration.class, SIZE, EVICTION_TYPE, EVICTION_STRATEGY, ADDRESS_COUNT);
   }

   public MemoryStorageConfiguration(AttributeSet attributes, StorageType storageType) {
      this.attributes = attributes;
      this.storageType = storageType;
      String storage = storageType == null ? StorageType.OBJECT.getElement().getLocalName() : storageType.getElement().getLocalName();
      this.elementDefinition = new DefaultElementDefinition(storage);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return elementDefinition;
   }

   public StorageType storageType() {
      return storageType;
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

   @Deprecated
   public int addressCount() {
      return attributes.attribute(ADDRESS_COUNT).get();
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
            ", storageType=" + storageType +
            '}';
   }
}
