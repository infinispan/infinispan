package org.infinispan.client.hotrod.configuration;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.util.Properties;
import java.util.regex.Pattern;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.util.TypedProperties;

public class NearCacheConfigurationBuilder extends AbstractConfigurationChildBuilder
      implements Builder<NearCacheConfiguration> {

   private NearCacheMode mode = NearCacheMode.DISABLED;
   private Integer maxEntries = null; // undefined
   private Pattern cacheNamePattern = null; // matches all

   protected NearCacheConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Specifies the maximum number of entries that will be held in the near cache.
    *
    * @param maxEntries maximum entries in the near cache.
    * @return an instance of the builder
    */
   public NearCacheConfigurationBuilder maxEntries(int maxEntries) {
      this.maxEntries = maxEntries;
      return this;
   }

   /**
    * Specifies the near caching mode. See {@link NearCacheMode} for details on the available modes.
    *
    * @param mode one of {@link NearCacheMode}
    * @return an instance of the builder
    */
   public NearCacheConfigurationBuilder mode(NearCacheMode mode) {
      this.mode = mode;
      return this;
   }

   /**
    * Specifies a cache name pattern (in the form of a regular expression) that matches all cache names for which
    * near caching should be enabled. See the {@link Pattern} syntax for details on the format.
    *
    * @param pattern a regular expression.
    * @return an instance of the builder
    */
   public NearCacheConfigurationBuilder cacheNamePattern(String pattern) {
      this.cacheNamePattern = Pattern.compile(pattern);
      return this;
   }

   /**
    * Specifies a cache name pattern that matches all cache names for which near caching should be enabled.
    *
    * @param pattern a {@link Pattern}
    * @return an instance of the builder
    */
   public NearCacheConfigurationBuilder cacheNamePattern(Pattern pattern) {
      this.cacheNamePattern = pattern;
      return this;
   }

   @Override
   public void validate() {
      if (mode.enabled() && maxEntries == null)
         throw HOTROD.nearCacheMaxEntriesUndefined();
   }

   @Override
   public NearCacheConfiguration create() {
      return new NearCacheConfiguration(mode, maxEntries == null ? -1 : maxEntries, cacheNamePattern);
   }

   @Override
   public Builder<?> read(NearCacheConfiguration template) {
      mode = template.mode();
      maxEntries = template.maxEntries();
      cacheNamePattern = template.cacheNamePattern();
      return this;
   }

   @Override
   public ConfigurationBuilder withProperties(Properties properties) {
      TypedProperties typed = TypedProperties.toTypedProperties(properties);
      if (typed.containsKey(ConfigurationProperties.NEAR_CACHE_MAX_ENTRIES)) {
         this.maxEntries(typed.getIntProperty(ConfigurationProperties.NEAR_CACHE_MAX_ENTRIES, -1));
      }
      if (typed.containsKey(ConfigurationProperties.NEAR_CACHE_MODE)) {
         this.mode(NearCacheMode.valueOf(typed.getProperty(ConfigurationProperties.NEAR_CACHE_MODE)));
      }
      if (typed.containsKey(ConfigurationProperties.NEAR_CACHE_NAME_PATTERN)) {
         this.cacheNamePattern(typed.getProperty(ConfigurationProperties.NEAR_CACHE_NAME_PATTERN));
      }
      return builder;
   }
}
