package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.executors.DefaultScheduledExecutorFactory;
import org.infinispan.executors.ScheduledExecutorFactory;

public class ScheduledExecutorFactoryConfiguration extends AbstractTypedPropertiesConfiguration {
   static final AttributeDefinition<ScheduledExecutorFactory> FACTORY = AttributeDefinition.builder("factory", (ScheduledExecutorFactory)new DefaultScheduledExecutorFactory()).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ExecutorFactoryConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(), FACTORY);
   }

   private final Attribute<ScheduledExecutorFactory> factory;

   ScheduledExecutorFactoryConfiguration(AttributeSet attributes) {
      super(attributes);
      factory = attributes.attribute(FACTORY);
   }

   public ScheduledExecutorFactory factory() {
      return factory.get();
   }

   AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "ScheduledExecutorFactoryConfiguration [attributes=" + attributes + "]";
   }
}
