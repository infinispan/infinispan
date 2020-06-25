package ${package};

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.CustomStoreConfigurationBuilder;

@BuiltBy(CustomStoreConfigurationBuilder.class)
@ConfigurationFor(CustomStore.class)
public class CustomStoreConfiguration extends AbstractStoreConfiguration {

   static final AttributeDefinition<String> SAMPLE_ATTRIBUTE = AttributeDefinition.builder("sampleAttribute", null, String.class).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(CustomStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), SAMPLE_ATTRIBUTE);
   }

   public CustomStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async) {
      super(attributes, async);
      // TODO Auto-generated constructor stub
   }
}
