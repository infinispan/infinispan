package ${package};

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

public class CustomModuleConfigurationBuilder implements Builder<CustomModuleConfiguration> {

   private final AttributeSet attributes = CustomModuleConfiguration.attributeDefinitionSet();

   private final GlobalConfigurationBuilder builder;

   public CustomModuleConfigurationBuilder(GlobalConfigurationBuilder builder) {
      this.builder = builder;
   }

   @Override
   public CustomModuleConfiguration create() {
      return new CustomModuleConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(CustomModuleConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public Builder<?> message(String message) {
      attributes.attribute(CustomModuleConfiguration.MESSAGE).set(message);
      return this;
   }
}
