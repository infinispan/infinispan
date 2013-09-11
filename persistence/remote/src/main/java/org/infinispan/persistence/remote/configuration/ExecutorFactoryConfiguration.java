package org.infinispan.persistence.remote.configuration;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.commons.util.TypedProperties;

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