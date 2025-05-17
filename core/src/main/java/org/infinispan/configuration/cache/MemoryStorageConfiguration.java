package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;

/**
 * @deprecated Since 11.0, {@link MemoryConfiguration} is used to defined the data container memory
 * eviction and sizing.
 */
@Deprecated(forRemoval=true, since = "11.0")
public class MemoryStorageConfiguration {

   public static final AttributeDefinition<Long> SIZE = AttributeDefinition.builder(Attribute.SIZE, -1L).build();
   public static final AttributeDefinition<EvictionType> EVICTION_TYPE = AttributeDefinition.builder(Element.EVICTION, EvictionType.COUNT).immutable().build();
   public static final AttributeDefinition<EvictionStrategy> EVICTION_STRATEGY = AttributeDefinition.builder(Attribute.STRATEGY, EvictionStrategy.NONE).immutable().build();
   public static final AttributeDefinition<StorageType> STORAGE_TYPE = AttributeDefinition.builder(Attribute.TYPE, StorageType.HEAP).immutable().build();

   private final AttributeSet attributes;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(MemoryStorageConfiguration.class, SIZE, EVICTION_TYPE, EVICTION_STRATEGY, STORAGE_TYPE);
   }

   public MemoryStorageConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
   }

   public AttributeSet attributes() {
      return attributes;
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
