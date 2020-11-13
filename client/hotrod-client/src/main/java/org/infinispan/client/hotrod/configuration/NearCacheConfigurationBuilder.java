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
   private boolean bloomFilter = false;

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
    * Specifies whether bloom filter should be used for near cache to limit the number of write
    * notifications for unrelated keys.
    * @param enable whether to enable bloom filter
    * @return an instance of this builder
    */
   public NearCacheConfigurationBuilder bloomFilter(boolean enable) {
      this.bloomFilter = enable;
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
    * @deprecated use {@link RemoteCacheConfigurationBuilder#nearCacheMode(NearCacheMode)} to enable near-caching per-cache
    */
   @Deprecated
   public NearCacheConfigurationBuilder cacheNamePattern(String pattern) {
      this.cacheNamePattern = Pattern.compile(pattern);
      return this;
   }

   /**
    * Specifies a cache name pattern that matches all cache names for which near caching should be enabled.
    *
    * @param pattern a {@link Pattern}
    * @return an instance of the builder
    * @deprecated use {@link RemoteCacheConfigurationBuilder#nearCacheMode(NearCacheMode)} to enable near-caching per-cache
    */
   @Deprecated
   public NearCacheConfigurationBuilder cacheNamePattern(Pattern pattern) {
      this.cacheNamePattern = pattern;
      return this;
   }

   @Override
   public void validate() {
      if (mode.enabled()) {
         if (maxEntries == null) {
            throw HOTROD.nearCacheMaxEntriesUndefined();
         } else if (maxEntries < 0 && bloomFilter) {
            throw HOTROD.nearCacheMaxEntriesPositiveWithBloom(maxEntries);
         }

         if (bloomFilter) {
            int maxActive = connectionPool().maxActive();
            ExhaustedAction exhaustedAction = connectionPool().exhaustedAction();
            if (maxActive != 1 || exhaustedAction != ExhaustedAction.WAIT) {
               throw HOTROD.bloomFilterRequiresMaxActiveOneAndWait(maxEntries, exhaustedAction);
            }
         }
      }
   }

   @Override
   public NearCacheConfiguration create() {
      return new NearCacheConfiguration(mode, maxEntries == null ? -1 : maxEntries, bloomFilter, cacheNamePattern);
   }

   @Override
   public Builder<?> read(NearCacheConfiguration template) {
      mode = template.mode();
      maxEntries = template.maxEntries();
      bloomFilter = template.bloomFilter();
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
      if (typed.containsKey(ConfigurationProperties.NEAR_CACHE_BLOOM_FILTER)) {
         this.bloomFilter(typed.getBooleanProperty(ConfigurationProperties.NEAR_CACHE_BLOOM_FILTER, false));
      }
      if (typed.containsKey(ConfigurationProperties.NEAR_CACHE_NAME_PATTERN)) {
         this.cacheNamePattern(typed.getProperty(ConfigurationProperties.NEAR_CACHE_NAME_PATTERN));
      }
      return builder;
   }
}
