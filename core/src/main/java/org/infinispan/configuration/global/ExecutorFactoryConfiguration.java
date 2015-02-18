package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.executors.DefaultExecutorFactory;

public class ExecutorFactoryConfiguration extends AbstractTypedPropertiesConfiguration {
   static final AttributeDefinition<ExecutorFactory> FACTORY = AttributeDefinition.builder("factory", (ExecutorFactory)new DefaultExecutorFactory()).immutable().build();
   public static AttributeSet attributeSet() {
      return new AttributeSet(ExecutorFactoryConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(), FACTORY);
   }

   ExecutorFactoryConfiguration(AttributeSet attributes) {
      super(attributes);
   }

   public ExecutorFactory factory() {
      return attributes.attribute(FACTORY).asObject(ExecutorFactory.class);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "ExecutorFactoryConfiguration [attributes=" + attributes + "]";
   }


}