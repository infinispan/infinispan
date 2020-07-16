package org.infinispan.anchored.configuration;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.Experimental;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;

/**
 * Configuration module builder to transform an {@link CacheMode#INVALIDATION_SYNC} cache into an "anchored keys" cache.
 *
 * <p>Usage:
 * <pre>
 * ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();
 * cacheBuilder.clustering().cacheMode(CacheMode.INVALIDATION_SYNC);
 * cacheBuilder.addModule(AnchoredKeysConfigurationBuilder.class).enabled(true);
 * </pre>
 * </p>
 * @see AnchoredKeysConfiguration
 *
 * @since 11
 * @author Dan Berindei
 */
@Experimental
public class AnchoredKeysConfigurationBuilder implements Builder<AnchoredKeysConfiguration> {
   private final AttributeSet attributes;
   private final ConfigurationBuilder rootBuilder;

   public AnchoredKeysConfigurationBuilder(ConfigurationBuilder builder) {
      rootBuilder = builder;
      this.attributes = AnchoredKeysConfiguration.attributeSet();
   }

   /**
    * Enable or disable anchored keys.
    */
   public void enabled(boolean enabled) {
      attributes.attribute(AnchoredKeysConfiguration.ENABLED).set(enabled);
   }

   @Override
   public void validate() {
      if (!rootBuilder.clustering().cacheMode().isReplicated()) {
         throw new CacheConfigurationException("Anchored keys requires cache to be in replication mode");
      }
      if (rootBuilder.transaction().transactionMode() != null && rootBuilder.transaction().transactionMode().isTransactional()) {
         throw new CacheConfigurationException("Anchored keys do not support transactions");
      }

      // TODO Maybe just assert that awaitInitialTransfer is disabled instead?
      rootBuilder.clustering().stateTransfer().awaitInitialTransfer(false);
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
}
