package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Controls the default expiration settings for entries in the cache.
 */
public class ExpirationConfiguration {
   static final AttributeDefinition<Long> LIFESPAN = AttributeDefinition.builder("lifespan", -1l).build();
   static final AttributeDefinition<Long> MAX_IDLE = AttributeDefinition.builder("maxIdle", -1l).build();
   static final AttributeDefinition<Boolean> REAPER_ENABLED = AttributeDefinition.builder("reaperEnabled", true).immutable().build();
   static final AttributeDefinition<Long> WAKEUP_INTERVAL = AttributeDefinition.builder("wakeUpInterval", TimeUnit.MINUTES.toMillis(1)).build();

   static AttributeSet attributeSet() {
      return new AttributeSet(ExpirationConfiguration.class, LIFESPAN, MAX_IDLE, REAPER_ENABLED, WAKEUP_INTERVAL);
   }
   private final AttributeSet attributes;

   ExpirationConfiguration(AttributeSet attributes) {
      attributes.checkProtection();
      this.attributes = attributes;
   }

   /**
    * Maximum lifespan of a cache entry, after which the entry is expired cluster-wide, in
    * milliseconds. -1 means the entries never expire.
    *
    * Note that this can be overridden on a per-entry basis by using the Cache API.
    */
   public long lifespan() {
      return attributes.attribute(LIFESPAN).asLong();
   }

   /**
    * Maximum idle time a cache entry will be maintained in the cache, in milliseconds. If the idle
    * time is exceeded, the entry will be expired cluster-wide. -1 means the entries never expire.
    *
    * Note that this can be overridden on a per-entry basis by using the Cache API.
    */
   public long maxIdle() {
      return attributes.attribute(MAX_IDLE).asLong();
   }

   /**
    * Determines whether the background reaper thread is enabled to test entries for expiration.
    * Regardless of whether a reaper is used, entries are tested for expiration lazily when they are
    * touched.
    */
   public boolean reaperEnabled() {
      return attributes.attribute(REAPER_ENABLED).asBoolean();
   }

   /**
    * Interval (in milliseconds) between subsequent runs to purge expired entries from memory and
    * any cache stores. If you wish to disable the periodic eviction process altogether, set
    * wakeupInterval to -1.
    */
   public long wakeUpInterval() {
      return attributes.attribute(WAKEUP_INTERVAL).asLong();
   }

   @Override
   public String toString() {
      return attributes.toString();
   }

   @Override
   public boolean equals(Object o) {
      ExpirationConfiguration other = (ExpirationConfiguration) o;
      return attributes.equals(other.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   AttributeSet attributes() {
      return attributes;
   }

}
