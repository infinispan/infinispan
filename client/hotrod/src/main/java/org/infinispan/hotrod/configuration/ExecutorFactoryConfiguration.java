package org.infinispan.hotrod.configuration;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.commons.util.TypedProperties;

/**
 * ExecutorFactoryConfiguration.
 *
 * @since 14.0
 */
public class ExecutorFactoryConfiguration extends AbstractTypedPropertiesConfiguration {

   private final Class<? extends ExecutorFactory> factoryClass;
   private final ExecutorFactory factory;

   @Deprecated
   ExecutorFactoryConfiguration(Class<? extends ExecutorFactory> factoryClass, TypedProperties properties) {
      super(properties);
      this.factoryClass = factoryClass;
      this.factory = null;
   }

   @Deprecated
   ExecutorFactoryConfiguration(ExecutorFactory factory, TypedProperties properties) {
      super(properties);
      this.factory = factory;
      this.factoryClass = null;
   }

   public Class<? extends ExecutorFactory> factoryClass() {
      return factoryClass;
   }

   public ExecutorFactory factory() {
      return factory;
   }

   @Override
   public String toString() {
      return "ExecutorFactoryConfiguration [factoryClass=" + factoryClass + ", factory=" + factory + ", properties()=" + properties() + "]";
   }

}
