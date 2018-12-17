package org.infinispan.configuration.cache;

import static org.infinispan.configuration.parsing.Element.MEMORY;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AsElementAttributeSerializer;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.IdentityAttributeCopier;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;

/**
 * Controls the memory storage configuration for the cache.
 *
 * @author William Burns
 */
public class MemoryConfiguration implements Matchable<MemoryConfiguration>, ConfigurationInfo {

   private static AttributeSerializer<StorageType, MemoryConfiguration, MemoryConfigurationBuilder> STORAGE_SERIALIZER = new AsElementAttributeSerializer<StorageType, MemoryConfiguration, MemoryConfigurationBuilder>() {
      @Override
      public boolean canRead(String enclosing, String nestingName, String nestedName, AttributeDefinition attributeDefinition) {
         return nestedName == null && StorageType.forElement(nestingName) != null;
      }

      @Override
      public String getParentElement(MemoryConfiguration configurationElement) {
         StorageType storageType = configurationElement.storageType();
         return storageType != null ? storageType.getElement().getLocalName() : null;
      }

      @Override
      public Object readAttributeValue(String enclosingElement, String nesting, AttributeDefinition attributeDefinition, Object attrValue, MemoryConfigurationBuilder builderInfo) {
         return StorageType.forElement(nesting);
      }

   };

   private static AttributeSerializer<Object, MemoryConfiguration, MemoryConfigurationBuilder> UNDER_STORAGE = new AttributeSerializer<Object, MemoryConfiguration, MemoryConfigurationBuilder>() {
      @Override
      public String getParentElement(MemoryConfiguration configurationElement) {
         StorageType storageType = configurationElement.storageType();
         return storageType != null ? storageType.getElement().getLocalName() : null;
      }

      @Override
      public boolean canRead(String enclosing, String nestingName, String nestedName, AttributeDefinition attributeDefinition) {
         return StorageType.forElement(nestingName) != null && nestedName.equals(attributeDefinition.xmlName());
      }
   };

   public static final AttributeDefinition<Integer> ADDRESS_COUNT = AttributeDefinition.builder("address-count", 1_048_576).serializer(UNDER_STORAGE).build();
   public static final AttributeDefinition<StorageType> STORAGE_TYPE = AttributeDefinition
         .builder("storage", StorageType.OBJECT).copier(IdentityAttributeCopier.INSTANCE)
         .serializer(STORAGE_SERIALIZER)
         .immutable().build();
   public static final AttributeDefinition<Long> SIZE = AttributeDefinition.builder("size", -1L).serializer(UNDER_STORAGE).build();
   public static final AttributeDefinition<EvictionType> EVICTION_TYPE = AttributeDefinition.builder("type", EvictionType.COUNT).xmlName(org.infinispan.configuration.parsing.Attribute.EVICTION.getLocalName()).serializer(UNDER_STORAGE).build();
   public static final AttributeDefinition<EvictionStrategy> EVICTION_STRATEGY = AttributeDefinition.builder("strategy", EvictionStrategy.NONE).serializer(UNDER_STORAGE).build();

   public static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(MEMORY.getLocalName());

   static public AttributeSet attributeDefinitionSet() {
      return new AttributeSet(MemoryConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(),
            STORAGE_TYPE, SIZE, EVICTION_TYPE, EVICTION_STRATEGY, ADDRESS_COUNT);
   }

   private final Attribute<Long> size;
   private final Attribute<EvictionType> evictionType;
   private final Attribute<EvictionStrategy> evictionStrategy;
   private final Attribute<StorageType> storageType;
   private final Attribute<Integer> addressCount;
   private final AttributeSet attributes;

   MemoryConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
      storageType = attributes.attribute(STORAGE_TYPE);
      size = attributes.attribute(SIZE);
      evictionType = attributes.attribute(EVICTION_TYPE);
      evictionStrategy = attributes.attribute(EVICTION_STRATEGY);
      addressCount = attributes.attribute(ADDRESS_COUNT);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
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

   /**
    * The configured eviction strategy
    * @return
    */
   public EvictionStrategy evictionStrategy() {
      return evictionStrategy.get();
   }

   /**
    * Returns whether remove eviction is in use
    * @return
    */
   public boolean isEvictionEnabled() {
      return size.get() > 0 && evictionStrategy.get().isRemovalBased();
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
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      MemoryConfiguration other = (MemoryConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

   @Override
   public String toString() {
      return "MemoryConfiguration [attributes=" + attributes + "]";
   }

}
