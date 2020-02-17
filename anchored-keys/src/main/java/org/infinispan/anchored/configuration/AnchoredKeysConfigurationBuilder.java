package org.infinispan.anchored.configuration;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;

public class AnchoredKeysConfigurationBuilder implements Builder<AnchoredKeysConfiguration> {
   private final AttributeSet attributes;
   private ConfigurationBuilder rootBuilder;

   public AnchoredKeysConfigurationBuilder(ConfigurationBuilder builder) {
      rootBuilder = builder;
      this.attributes = AnchoredKeysConfiguration.attributeSet();
   }

   @Override
   public void validate() {
      if (rootBuilder.clustering().cacheMode() != CacheMode.INVALIDATION_SYNC) {
         throw new CacheConfigurationException("Anchored keys requires cache mode to be INVALIDATION_SYNC");
      }
      if (rootBuilder.transaction().transactionMode() != null && rootBuilder.transaction().transactionMode().isTransactional()) {
         throw new CacheConfigurationException("Anchored keys do not support transactions");
      }
   }

   @Override
   public AnchoredKeysConfiguration create() {
      return new AnchoredKeysConfiguration(attributes);
   }

   @Override
   public Builder<?> read(AnchoredKeysConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   public void enabled(boolean enabled) {
      attributes.attribute(AnchoredKeysConfiguration.ENABLED).set(enabled);
   }
}
