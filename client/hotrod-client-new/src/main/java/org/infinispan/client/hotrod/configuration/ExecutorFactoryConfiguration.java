package org.infinispan.client.hotrod.configuration;

import org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory;
import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.executors.ExecutorFactory;

/**
 * ExecutorFactoryConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class ExecutorFactoryConfiguration extends AbstractTypedPropertiesConfiguration {

   static final AttributeDefinition<ExecutorFactory> FACTORY = AttributeDefinition.builder("factory", (ExecutorFactory) new DefaultAsyncExecutorFactory()).immutable().build();
   static final AttributeDefinition<Class<? extends ExecutorFactory>> FACTORY_CLASS = AttributeDefinition.classBuilder("factoryClass", ExecutorFactory.class).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ExecutorFactoryConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(), FACTORY, FACTORY_CLASS);
   }

   ExecutorFactoryConfiguration(AttributeSet attributes) {
      super(attributes);
   }

   public ExecutorFactory factory() {
      return attributes.attribute(FACTORY).get();
   }

   public Class<? extends ExecutorFactory> factoryClass() {
      return attributes.attribute(FACTORY_CLASS).get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "ExecutorFactoryConfiguration [attributes=" + attributes + "]";
   }
}
