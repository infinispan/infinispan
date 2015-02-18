package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.executors.DefaultExecutorFactory;

public class ExecutorFactoryConfiguration extends AbstractTypedPropertiesConfiguration {
   static final AttributeDefinition<ExecutorFactory> FACTORY = AttributeDefinition.builder("factory", (ExecutorFactory)new DefaultExecutorFactory()).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ExecutorFactoryConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(), FACTORY);
   }

   private final Attribute<ExecutorFactory> factory;

   ExecutorFactoryConfiguration(AttributeSet attributes) {
      super(attributes);
      factory = attributes.attribute(FACTORY);
   }

   public ExecutorFactory factory() {
      return factory.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "ExecutorFactoryConfiguration [attributes=" + attributes + "]";
   }


}