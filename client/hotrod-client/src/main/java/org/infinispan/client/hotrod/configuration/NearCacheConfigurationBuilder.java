package org.infinispan.client.hotrod.configuration;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import org.infinispan.client.hotrod.near.DefaultNearCacheFactory;
import org.infinispan.client.hotrod.near.NearCacheFactory;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class NearCacheConfigurationBuilder extends AbstractConfigurationChildBuilder
      implements Builder<NearCacheConfiguration> {

   private NearCacheMode mode = NearCacheMode.DISABLED;
   private Integer maxEntries = null; // undefined
   private boolean bloomFilter = false;
   private NearCacheFactory nearCacheFactory = DefaultNearCacheFactory.INSTANCE;

   protected NearCacheConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
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
    * Specifies a {@link NearCacheFactory} which is responsible for creating {@link org.infinispan.client.hotrod.near.NearCache} instances.
    *
    * @param factory a {@link NearCacheFactory}
    * @return an instance of the builder
    */
   public NearCacheConfigurationBuilder nearCacheFactory(NearCacheFactory factory) {
      this.nearCacheFactory = factory;
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
      }
   }

   @Override
   public NearCacheConfiguration create() {
      return new NearCacheConfiguration(mode, maxEntries == null ? -1 : maxEntries, bloomFilter, nearCacheFactory);
   }

   @Override
   public Builder<?> read(NearCacheConfiguration template, Combine combine) {
      mode = template.mode();
      maxEntries = template.maxEntries();
      bloomFilter = template.bloomFilter();
      nearCacheFactory = template.nearCacheFactory();
      return this;
   }
}
