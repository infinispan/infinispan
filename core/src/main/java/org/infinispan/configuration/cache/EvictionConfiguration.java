package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;

/**
 * Controls the eviction settings for the cache.
 */
public class EvictionConfiguration {
   static final AttributeDefinition<Integer> MAX_ENTRIES  = AttributeDefinition.builder("maxEntries", -1).build();
   static final AttributeDefinition<EvictionStrategy> STRATEGY = AttributeDefinition.builder("strategy", EvictionStrategy.NONE).immutable().build();
   static final AttributeDefinition<EvictionThreadPolicy> THREAD_POLICY = AttributeDefinition.builder("threadPolicy", EvictionThreadPolicy.DEFAULT).immutable().build();
   static AttributeSet attributeSet() {
      return new AttributeSet(EvictionConfiguration.class, MAX_ENTRIES, STRATEGY, THREAD_POLICY);
   }
   private final AttributeSet attributes;

   EvictionConfiguration(AttributeSet attributes) {
      attributes.checkProtection();
      this.attributes = attributes;
   }

   /**
    * Eviction strategy. Available options are 'UNORDERED', 'LRU', 'LIRS' and 'NONE' (to disable
    * eviction).
    */
   public EvictionStrategy strategy() {
      return attributes.attribute(STRATEGY).get();
   }

   /**
    * Threading policy for eviction.
    */
   public EvictionThreadPolicy threadPolicy() {
      return attributes.attribute(THREAD_POLICY).asObject(EvictionThreadPolicy.class);
   }

   /**
    * Maximum number of entries in a cache instance. Cache size is guaranteed not to exceed upper
    * limit specified by max entries. However, due to the nature of eviction it is unlikely to ever
    * be exactly maximum number of entries specified here.
    */
   public int maxEntries() {
      return attributes.attribute(MAX_ENTRIES).asInteger();
   }

   AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return attributes.toString();
   }

   @Override
   public boolean equals(Object o) {
      EvictionConfiguration other = (EvictionConfiguration) o;
      return attributes.equals(other.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

}
