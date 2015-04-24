package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.eviction.EvictionType;

/**
 * Controls the eviction settings for the cache.
 */
public class EvictionConfiguration {
   public static final AttributeDefinition<Long> SIZE  = AttributeDefinition.builder("size", -1l).build();
   public static final AttributeDefinition<EvictionType> TYPE  = AttributeDefinition.builder("type", EvictionType.COUNT).build();
   public static final AttributeDefinition<EvictionStrategy> STRATEGY = AttributeDefinition.builder("strategy", EvictionStrategy.NONE).immutable().build();
   public static final AttributeDefinition<EvictionThreadPolicy> THREAD_POLICY = AttributeDefinition.builder("threadPolicy", EvictionThreadPolicy.DEFAULT).immutable().build();
   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(EvictionConfiguration.class, SIZE,
            TYPE, STRATEGY, THREAD_POLICY);
   }

   private final Attribute<Long> size;
   private final Attribute<EvictionType> type;
   private final Attribute<EvictionStrategy> strategy;
   private final Attribute<EvictionThreadPolicy> threadPolicy;
   private final AttributeSet attributes;

   EvictionConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      size = attributes.attribute(SIZE);
      type = attributes.attribute(TYPE);
      strategy = attributes.attribute(STRATEGY);
      threadPolicy = attributes.attribute(THREAD_POLICY);
   }

   /**
    * Eviction strategy. Available options are 'UNORDERED', 'LRU', 'LIRS' and 'NONE' (to disable
    * eviction).
    */
   public EvictionStrategy strategy() {
      return strategy.get();
   }

   /**
    * Threading policy for eviction.
    */
   public EvictionThreadPolicy threadPolicy() {
      return threadPolicy.get();
   }

   /**
    * Maximum number of entries in a cache instance. Cache size is guaranteed not to exceed upper
    * limit specified by max entries. However, due to the nature of eviction it is unlikely to ever
    * be exactly maximum number of entries specified here. Only makes sense when using the
    * COUNT type.
    */
   public long maxEntries() {
      if (type.get() != EvictionType.COUNT) {
         throw new IllegalStateException();
      }
      return size();
   }

   public long size() {
      return size.get();
   }

   public EvictionType type() {
      return type.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "EvictionConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      EvictionConfiguration other = (EvictionConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

}
