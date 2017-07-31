package org.infinispan.configuration.internal;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

/**
 * A {@link Builder} implementation of {@link PrivateGlobalConfiguration}.
 *
 * @author Pedro Ruivo
 * @see PrivateGlobalConfiguration
 * @since 9.0
 */
public class PrivateGlobalConfigurationBuilder implements Builder<PrivateGlobalConfiguration> {

   private final AttributeSet attributes;

   public PrivateGlobalConfigurationBuilder(GlobalConfigurationBuilder builder) {
      this.attributes = PrivateGlobalConfiguration.attributeSet();
   }

   public PrivateGlobalConfigurationBuilder serverMode(boolean serverMode) {
      this.attributes.attribute(PrivateGlobalConfiguration.SERVER_MODE).set(serverMode);
      return this;
   }

   @Override
   public void validate() {
      //nothing
   }

   @Override
   public PrivateGlobalConfiguration create() {
      return new PrivateGlobalConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(PrivateGlobalConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "PrivateGlobalConfigurationBuilder [attributes=" + attributes + ']';
   }
}
