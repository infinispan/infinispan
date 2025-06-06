package org.infinispan.query.impl.config;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractModuleConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;

/**
 * @api.private
 */
public class PrivateIndexingConfigurationBuilder extends AbstractModuleConfigurationBuilder implements Builder<PrivateIndexingConfiguration> {

   private final AttributeSet attributes;

   protected PrivateIndexingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = PrivateIndexingConfiguration.attributeDefinitionSet();
   }

   @Override
   public PrivateIndexingConfiguration create() {
      return new PrivateIndexingConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(PrivateIndexingConfiguration template, Combine combine) {
      this.attributes.read(template.attributes, combine);
      return this;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public PrivateIndexingConfigurationBuilder rebatchRequestsSize(int rebatchRequestsSize) {
      this.attributes.attribute(PrivateIndexingConfiguration.REBATCH_REQUESTS_SIZE).set(rebatchRequestsSize);
      return this;
   }
}
