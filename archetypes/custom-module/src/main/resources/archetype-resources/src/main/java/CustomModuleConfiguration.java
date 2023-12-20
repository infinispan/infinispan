package ${package};

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.serializing.SerializedWith;

@BuiltBy(CustomModuleConfigurationBuilder.class)
@SerializedWith(CustomModuleSerializer.class)
public class CustomModuleConfiguration extends ConfigurationElement<CustomModuleConfiguration> {

   static final AttributeDefinition<String> MESSAGE = AttributeDefinition.builder(Attribute.MESSAGE, "Module Loaded")
         .immutable().build();
   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(CustomModuleConfiguration.class, MESSAGE);
   }

   CustomModuleConfiguration(AttributeSet attributes) {
      super(Element.ROOT, attributes);
   }

   public String message() {
      return attributes.attribute(MESSAGE).get();
   }
}
