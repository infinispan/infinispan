package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 * @deprecated since 11.0, use {@link MemoryConfigurationBuilder} instead.
 */
@Deprecated
public class MemoryStorageConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<MemoryStorageConfiguration> {
   AttributeSet attributes;

   MemoryStorageConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = MemoryStorageConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public MemoryStorageConfiguration create() {
      return new MemoryStorageConfiguration(attributes.protect());
   }

   @Override
   public MemoryStorageConfigurationBuilder read(MemoryStorageConfiguration template, Combine combine) {
      return this;
   }

   @Override
   public String toString() {
      return "MemoryStorageConfigurationBuilder [attributes=" + attributes + "]";
   }
}
