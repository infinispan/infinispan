package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * @since 10.0
 * @deprecated since 11.0, use {@link MemoryConfigurationBuilder} instead.
 */
@Deprecated
public class MemoryStorageConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<MemoryStorageConfiguration>, ConfigurationBuilderInfo {
   private AttributeSet attributes;

   MemoryStorageConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = MemoryStorageConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition<?> getElementDefinition() {
      return null;
   }

   @Override
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public MemoryStorageConfiguration create() {
      return new MemoryStorageConfiguration(attributes.protect());
   }

   @Override
   public MemoryStorageConfigurationBuilder read(MemoryStorageConfiguration template) {
      return this;
   }

   @Override
   public String toString() {
      return "MemoryStorageConfigurationBuilder [attributes=" + attributes + "]";
   }
}
