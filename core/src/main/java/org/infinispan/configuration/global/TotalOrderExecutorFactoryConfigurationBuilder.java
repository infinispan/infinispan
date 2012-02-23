package org.infinispan.configuration.global;

import org.infinispan.executors.DefaultDynamicExecutorFactory;

import java.util.Properties;

/**
 * Sets another default ExecutorFactory for the Total Order executor.
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderExecutorFactoryConfigurationBuilder extends ExecutorFactoryConfigurationBuilder {
   TotalOrderExecutorFactoryConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      //By default, I want a dynamic thread pool
      factory(new DefaultDynamicExecutorFactory());
   }

   @Override
   void validate() {
      Properties properties = getProperties();
      if (!properties.contains("threadNamePrefix")) {
         properties.put("threadNamePrefix", "Total-Order-Validation");
      }
   }
}
