package org.infinispan.hotrod.configuration;

import static org.infinispan.hotrod.configuration.NearCacheConfiguration.BLOOM_FILTER;
import static org.infinispan.hotrod.configuration.NearCacheConfiguration.FACTORY;
import static org.infinispan.hotrod.configuration.NearCacheConfiguration.MAX_ENTRIES;
import static org.infinispan.hotrod.configuration.NearCacheConfiguration.MODE;
import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.util.Properties;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.hotrod.impl.ConfigurationProperties;

public class NearCacheConfigurationBuilder extends AbstractConfigurationChildBuilder
      implements Builder<NearCacheConfiguration> {
   private final AttributeSet attributes = NearCacheConfiguration.attributeDefinitionSet();

   NearCacheConfigurationBuilder(HotRodConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Specifies the maximum number of entries that will be held in the near cache.
    *
    * @param maxEntries maximum entries in the near cache.
    * @return an instance of the builder
    */
   public NearCacheConfigurationBuilder maxEntries(int maxEntries) {
      attributes.attribute(MAX_ENTRIES).set(maxEntries);
      return this;
   }

   /**
    * Specifies whether bloom filter should be used for near cache to limit the number of write
    * notifications for unrelated keys.
    * @param enable whether to enable bloom filter
    * @return an instance of this builder
    */
   public NearCacheConfigurationBuilder bloomFilter(boolean enable) {
      attributes.attribute(BLOOM_FILTER).set(enable);
      return this;
   }

   /**
    * Specifies the near caching mode. See {@link NearCacheMode} for details on the available modes.
    *
    * @param mode one of {@link NearCacheMode}
    * @return an instance of the builder
    */
   public NearCacheConfigurationBuilder mode(NearCacheMode mode) {
      attributes.attribute(MODE).set(mode);
      return this;
   }

   /**
    * Specifies a {@link NearCacheFactory} which is responsible for creating near cache instances.
    *
    * @param factory a {@link NearCacheFactory}
    * @return an instance of the builder
    */
   public NearCacheConfigurationBuilder nearCacheFactory(NearCacheFactory factory) {
      attributes.attribute(FACTORY).set(factory);
      return this;
   }

   @Override
   public void validate() {
      if (attributes.attribute(MODE).get().enabled()) {
         if (attributes.attribute(MAX_ENTRIES).isNull()) {
            throw HOTROD.nearCacheMaxEntriesUndefined();
         }
         int maxEntries = attributes.attribute(MAX_ENTRIES).get();
         boolean bloomFilter = attributes.attribute(BLOOM_FILTER).get();
         if (maxEntries < 0 && bloomFilter) {
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
      return new NearCacheConfiguration(attributes.protect());
   }

   @Override
   public NearCacheConfigurationBuilder read(NearCacheConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public HotRodConfigurationBuilder withProperties(Properties properties) {
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
      return builder;
   }
}
