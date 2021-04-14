package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.expiration.TouchMode;

/**
 * Controls the default expiration settings for entries in the cache.
 */
public class ExpirationConfiguration extends ConfigurationElement<ExpirationConfiguration> {
   public static final AttributeDefinition<Long> LIFESPAN = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.LIFESPAN, -1L).build();
   public static final AttributeDefinition<Long> MAX_IDLE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.MAX_IDLE, -1L).build();
   public static final AttributeDefinition<Boolean> REAPER_ENABLED = AttributeDefinition.builder("reaperEnabled", true).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Long> WAKEUP_INTERVAL = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.INTERVAL, TimeUnit.MINUTES.toMillis(1)).build();
   public static final AttributeDefinition<TouchMode> TOUCH = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.TOUCH, TouchMode.SYNC).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ExpirationConfiguration.class, LIFESPAN, MAX_IDLE, REAPER_ENABLED, WAKEUP_INTERVAL, TOUCH);
   }

   private final Attribute<Long> lifespan;
   private final Attribute<Long> maxIdle;
   private final Attribute<Boolean> reaperEnabled;
   private final Attribute<Long> wakeUpInterval;
   private final Attribute<TouchMode> touch;

   ExpirationConfiguration(AttributeSet attributes) {
      super(Element.EXPIRATION, attributes);
      lifespan = attributes.attribute(LIFESPAN);
      maxIdle = attributes.attribute(MAX_IDLE);
      reaperEnabled = attributes.attribute(REAPER_ENABLED);
      wakeUpInterval = attributes.attribute(WAKEUP_INTERVAL);
      touch = attributes.attribute(TOUCH);
   }

   /**
    * Maximum lifespan of a cache entry, after which the entry is expired cluster-wide, in
    * milliseconds. -1 means the entries never expire.
    *
    * Note that this can be overridden on a per-entry basis by using the Cache API.
    */
   public long lifespan() {
      return lifespan.get();
   }

   /**
    * Maximum idle time a cache entry will be maintained in the cache, in milliseconds. If the idle
    * time is exceeded, the entry will be expired cluster-wide. -1 means the entries never expire.
    *
    * Note that this can be overridden on a per-entry basis by using the Cache API.
    */
   public long maxIdle() {
      return maxIdle.get();
   }

   /**
    * Determines whether the background reaper thread is enabled to test entries for expiration.
    * Regardless of whether a reaper is used, entries are tested for expiration lazily when they are
    * touched.
    */
   public boolean reaperEnabled() {
      return reaperEnabled.get();
   }

   /**
    * Interval (in milliseconds) between subsequent runs to purge expired entries from memory and
    * any cache stores. If you wish to disable the periodic eviction process altogether, set
    * wakeupInterval to -1.
    */
   public long wakeUpInterval() {
      return wakeUpInterval.get();
   }

   /**
    * Control how the timestamp of read keys are updated on all the key owners in a cluster.
    *
    * Default is {@link TouchMode#SYNC}.
    * If the cache mode is ASYNC this attribute is ignored, and timestamps are updated asynchronously.
    */
   public TouchMode touch() {
      return touch.get();
   }
}
