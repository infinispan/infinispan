package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.executors.ScheduledExecutorFactory;

public class ScheduledExecutorFactoryConfiguration extends AbstractTypedPropertiesConfiguration {

   private final ScheduledExecutorFactory factory;

   ScheduledExecutorFactoryConfiguration(ScheduledExecutorFactory factory, TypedProperties properties) {
      super(properties);
      this.factory = factory;
   }

   public ScheduledExecutorFactory factory() {
      return factory;
   }

   @Override
   public String toString() {
      return "ScheduledExecutorFactoryConfiguration{" +
            "factory=" + factory +
            '}';
   }

}