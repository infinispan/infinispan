package org.infinispan.configuration.global;

import org.infinispan.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.util.TypedProperties;

public class ExecutorFactoryConfiguration extends AbstractTypedPropertiesConfiguration {

   private final ExecutorFactory factory;
   
   ExecutorFactoryConfiguration(ExecutorFactory factory, TypedProperties properties) {
      super(properties);
      this.factory = factory;
   }

   public ExecutorFactory factory() {
      return factory;
   }

   @Override
   public String toString() {
      return "ExecutorFactoryConfiguration{" +
            "factory=" + factory +
            '}';
   }

}