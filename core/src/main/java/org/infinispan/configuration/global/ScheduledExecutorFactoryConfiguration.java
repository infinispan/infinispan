package org.infinispan.configuration.global;

import org.infinispan.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.executors.ScheduledExecutorFactory;
import org.infinispan.util.TypedProperties;

public class ScheduledExecutorFactoryConfiguration extends AbstractTypedPropertiesConfiguration {

   private final ScheduledExecutorFactory factory;

   ScheduledExecutorFactoryConfiguration(ScheduledExecutorFactory factory, TypedProperties properties) {
      super(properties);
      this.factory = factory;
   }

   public ScheduledExecutorFactory getFactory() {
      return factory;
   }

}