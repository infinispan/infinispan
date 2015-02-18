package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;

/**
 * Controls the eviction settings for the cache.
 */
public class EvictionConfiguration {
   public static final AttributeDefinition<Integer> MAX_ENTRIES  = AttributeDefinition.builder("maxEntries", -1).build();
   public static final AttributeDefinition<EvictionStrategy> STRATEGY = AttributeDefinition.builder("strategy", EvictionStrategy.NONE).immutable().build();
   public static final AttributeDefinition<EvictionThreadPolicy> THREAD_POLICY = AttributeDefinition.builder("threadPolicy", EvictionThreadPolicy.DEFAULT).immutable().build();
   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(EvictionConfiguration.class, MAX_ENTRIES, STRATEGY, THREAD_POLICY);
   }

   private final Attribute<Integer> maxEntries;
   private final Attribute<EvictionStrategy> strategy;
   private final Attribute<EvictionThreadPolicy> threadPolicy;
   private final AttributeSet attributes;

   EvictionConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      maxEntries = attributes.attribute(MAX_ENTRIES);
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
    * be exactly maximum number of entries specified here.
    */
   public int maxEntries() {
      return maxEntries.get();
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
