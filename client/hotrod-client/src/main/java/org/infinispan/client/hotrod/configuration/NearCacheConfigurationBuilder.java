package org.infinispan.client.hotrod.configuration;

import java.util.regex.Pattern;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.Builder;

public class NearCacheConfigurationBuilder extends AbstractConfigurationChildBuilder
      implements Builder<NearCacheConfiguration> {
   private static final Log log = LogFactory.getLog(NearCacheConfigurationBuilder.class);

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
         throw log.nearCacheMaxEntriesUndefined();
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
}
