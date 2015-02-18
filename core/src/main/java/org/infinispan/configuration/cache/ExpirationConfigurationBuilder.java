package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.ExpirationConfiguration.*;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * Controls the default expiration settings for entries in the cache.
 */
public class ExpirationConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<ExpirationConfiguration> {


   private final AttributeSet attributes;

   ExpirationConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = ExpirationConfiguration.attributeDefinitionSet();
   }

   /**
    * Maximum lifespan of a cache entry, after which the entry is expired cluster-wide, in
    * milliseconds. -1 means the entries never expire.
    *
    * Note that this can be overridden on a per-entry basis by using the Cache API.
    */
   public ExpirationConfigurationBuilder lifespan(long l) {
      attributes.attribute(LIFESPAN).set(l);
      return this;
   }

   /**
    * Maximum lifespan of a cache entry, after which the entry is expired cluster-wide, in
    * milliseconds. -1 means the entries never expire.
    *
    * Note that this can be overridden on a per-entry basis by using the Cache API.
    */
   public ExpirationConfigurationBuilder lifespan(long l, TimeUnit unit) {
      return lifespan(unit.toMillis(l));
   }

   /**
    * Maximum idle time a cache entry will be maintained in the cache, in milliseconds. If the idle
    * time is exceeded, the entry will be expired cluster-wide. -1 means the entries never expire.
    *
    * Note that this can be overridden on a per-entry basis by using the Cache API.
    */
   public ExpirationConfigurationBuilder maxIdle(long l) {
      attributes.attribute(MAX_IDLE).set(l);
      return this;
   }

   /**
    * Maximum idle time a cache entry will be maintained in the cache, in milliseconds. If the idle
    * time is exceeded, the entry will be expired cluster-wide. -1 means the entries never expire.
    *
    * Note that this can be overridden on a per-entry basis by using the Cache API.
    */
   public ExpirationConfigurationBuilder maxIdle(long l, TimeUnit unit) {
      return maxIdle(unit.toMillis(l));
   }

   /**
    * Enable the background reaper to test entries for expiration.
    * Regardless of whether a reaper is used, entries are tested for expiration lazily when they are
    * touched.
    */
   public ExpirationConfigurationBuilder enableReaper() {
      attributes.attribute(REAPER_ENABLED).set(true);
      return this;
   }

   /**
    * Enable the background reaper to test entries for expiration.
    * Regardless of whether a reaper is used, entries are tested for expiration lazily when they are
    * touched.
    */
   public ExpirationConfigurationBuilder reaperEnabled(boolean enabled) {
      attributes.attribute(REAPER_ENABLED).set(enabled);
      return this;
   }

   /**
    * Disable the background reaper to test entries for expiration. to test entries for expiration.
    * Regardless of whether a reaper is used, entries are tested for expiration lazily when they are
    * touched.
    */
   public ExpirationConfigurationBuilder disableReaper() {
      attributes.attribute(REAPER_ENABLED).set(false);
      return this;
   }

   /**
    * Interval (in milliseconds) between subsequent runs to purge expired entries from memory and
    * any cache stores. If you wish to disable the periodic eviction process altogether, set
    * wakeupInterval to -1.
    */
   public ExpirationConfigurationBuilder wakeUpInterval(long l) {
      attributes.attribute(WAKEUP_INTERVAL).set(l);
      return this;
   }

   /**
    * Interval (in milliseconds) between subsequent runs to purge expired entries from memory and
    * any cache stores. If you wish to disable the periodic eviction process altogether, set
    * wakeupInterval to -1.
    */
   public ExpirationConfigurationBuilder wakeUpInterval(long l, TimeUnit unit) {
      return wakeUpInterval(unit.toMillis(l));
   }

   @Override
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public ExpirationConfiguration create() {
      return new ExpirationConfiguration(attributes.protect());
   }

   @Override
   public ExpirationConfigurationBuilder read(ExpirationConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return this.getClass().getSimpleName() + attributes;
   }
}
