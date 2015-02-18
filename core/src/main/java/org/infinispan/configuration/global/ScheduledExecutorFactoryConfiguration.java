package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.executors.DefaultScheduledExecutorFactory;
import org.infinispan.executors.ScheduledExecutorFactory;

public class ScheduledExecutorFactoryConfiguration extends AbstractTypedPropertiesConfiguration {
   static final AttributeDefinition<ScheduledExecutorFactory> FACTORY = AttributeDefinition.builder("factory", (ScheduledExecutorFactory)new DefaultScheduledExecutorFactory()).immutable().build();
   public static AttributeSet attributeSet() {
      return new AttributeSet(ExecutorFactoryConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(), FACTORY);
   }

   ScheduledExecutorFactoryConfiguration(AttributeSet attributes) {
      super(attributes);
   }

   public ScheduledExecutorFactory factory() {
      return attributes.attribute(FACTORY).asObject(ScheduledExecutorFactory.class);
   }

   AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "ScheduledExecutorFactoryConfiguration [attributes=" + attributes + "]";
   }
}
