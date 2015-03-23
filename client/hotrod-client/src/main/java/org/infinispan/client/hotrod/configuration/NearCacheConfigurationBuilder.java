package org.infinispan.client.hotrod.configuration;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.Builder;

public class NearCacheConfigurationBuilder extends AbstractConfigurationChildBuilder
      implements Builder<NearCacheConfiguration> {
   private static final Log log = LogFactory.getLog(NearCacheConfigurationBuilder.class);

   private NearCacheMode mode = NearCacheMode.DISABLED;
   private Integer maxEntries = null; // undefined

   protected NearCacheConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   public NearCacheConfigurationBuilder maxEntries(int maxEntries) {
      this.maxEntries = maxEntries;
      return this;
   }

   public NearCacheConfigurationBuilder mode(NearCacheMode mode) {
      this.mode = mode;
      return this;
   }

   @Override
   public void validate() {
      if (mode.enabled() && maxEntries == null)
         throw log.nearCacheMaxEntriesUndefined();
   }

   @Override
   public NearCacheConfiguration create() {
      return new NearCacheConfiguration(mode, maxEntries == null ? -1 : maxEntries.intValue());
   }

   @Override
   public Builder<?> read(NearCacheConfiguration template) {
      mode = template.mode();
      maxEntries = template.maxEntries();
      return this;
   }
}
