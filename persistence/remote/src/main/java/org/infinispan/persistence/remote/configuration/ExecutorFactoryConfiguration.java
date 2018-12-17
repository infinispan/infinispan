package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.Element.ASYNC_TRANSPORT_EXECUTOR;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.executors.DefaultExecutorFactory;

public class ExecutorFactoryConfiguration extends AbstractTypedPropertiesConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<ExecutorFactory> EXECUTOR_FACTORY = AttributeDefinition.builder("executorFactory", null, ExecutorFactory.class)
         .initializer(DefaultExecutorFactory::new).xmlName("factory").immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ExecutorFactoryConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(), EXECUTOR_FACTORY);
   };

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(ASYNC_TRANSPORT_EXECUTOR.getLocalName());

   ExecutorFactoryConfiguration(AttributeSet attributes) {
      super(attributes);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public ExecutorFactory factory() {
      return attributes.attribute(EXECUTOR_FACTORY).get();
   }

   @Override
   public String toString() {
      return "ExecutorFactoryConfiguration [attributes=" + attributes + "]";
   }

   public AttributeSet attributes() {
      return attributes;
   }

}
