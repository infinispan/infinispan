package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.IdentityAttributeCopier;
import org.infinispan.eviction.EvictionType;

/**
 * Controls the memory storage configuration for the cache.
 *
 * @author William Burns
 */
public class MemoryConfiguration {
   public static final AttributeDefinition<Integer> ADDRESS_COUNT = AttributeDefinition.builder("address-count", 1_048_576).build();
   public static final AttributeDefinition<StorageType> STORAGE_TYPE = AttributeDefinition
         .builder("storage", StorageType.OBJECT).copier(IdentityAttributeCopier.INSTANCE).immutable().build();
   public static final AttributeDefinition<Long> SIZE  = AttributeDefinition.builder("size", -1L).build();
   public static final AttributeDefinition<EvictionType> EVICTION_TYPE  = AttributeDefinition.builder("type", EvictionType.COUNT).build();

   static public AttributeSet attributeDefinitionSet() {
      return new AttributeSet(MemoryConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(),
            STORAGE_TYPE, SIZE, EVICTION_TYPE, ADDRESS_COUNT);
   }

   private final Attribute<Long> size;
   private final Attribute<EvictionType> evictionType;
   private final Attribute<StorageType> storageType;
   private final Attribute<Integer> addressCount;
   private final AttributeSet attributes;

   MemoryConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
      storageType = attributes.attribute(STORAGE_TYPE);
      size = attributes.attribute(SIZE);
      evictionType = attributes.attribute(EVICTION_TYPE);
      addressCount = attributes.attribute(ADDRESS_COUNT);
   }

   /**
    * Storage type to use for the data container
    * @return
    */
   public StorageType storageType() {
      return storageType.get();
   }

   /**
    * Size of the eviction, -1 if disabled
    * @return
    */
   public long size() {
      return size.get();
   }

   public void size(long newSize) {
      size.set(newSize);
   }

   /**
    * The configured eviction type
    * @return
    */
   public EvictionType evictionType() {
      return evictionType.get();
   }

   public boolean isEvictionEnabled() {
      return size.get() > 0;
   }

   /**
    * The address pointer count
    * @return
    */
   public int addressCount() {
      return addressCount.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "MemoryConfiguration [attributes=" + attributes + "]";
   }

}
