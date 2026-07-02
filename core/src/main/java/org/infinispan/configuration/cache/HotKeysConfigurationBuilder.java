package org.infinispan.configuration.cache;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Builder for {@link HotKeysConfiguration}.
 *
 * @since 16.4
 */
public class HotKeysConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<HotKeysConfiguration> {

   private final AttributeSet attributes;

   HotKeysConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.attributes = HotKeysConfiguration.attributeDefinitions();
   }

   /**
    * Enable or disable hot key tracking.
    *
    * @param enabled {@code true} to activate tracking
    * @return this builder
    */
   public HotKeysConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(HotKeysConfiguration.ENABLED).set(enabled);
      return this;
   }

   /**
    * Sets the number of top keys to track per operation type.
    *
    * @param k the top-k value, between 1 and {@link HotKeysConfiguration#MAX_TOP_K}
    * @return this builder
    * @throws CacheConfigurationException if {@code k} is out of range
    */
   public HotKeysConfigurationBuilder topK(int k) {
      attributes.attribute(HotKeysConfiguration.TOP_K).set(k);
      return this;
   }

   @Override
   public void validate() {
      attributes.validate();
   }

   @Override
   public HotKeysConfiguration create() {
      return new HotKeysConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(HotKeysConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "HotKeysConfigurationBuilder{attributes=" + attributes + '}';
   }
}
