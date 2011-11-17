package org.infinispan.configuration.global;

import java.util.Properties;

import org.infinispan.executors.DefaultExecutorFactory;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.util.TypedProperties;

/**
 * Configures executor factory.
 */
public class ExecutorFactoryConfigurationBuilder extends AbstractGlobalConfigurationBuilder<ExecutorFactoryConfiguration> {
   
   private ExecutorFactory factory = new DefaultExecutorFactory();
   private Properties properties;
   
   ExecutorFactoryConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      this.properties = new Properties();
   }
   
   /**
    * Specify factory class for executor
    *
    * @param factory clazz
    * @return this ExecutorFactoryConfig
    */
   public ExecutorFactoryConfigurationBuilder factory(ExecutorFactory factory) {
      this.factory = factory;
      return this;
   }

   /**
    * Add key/value property pair to this executor factory configuration
    *
    * @param key   property key
    * @param value property value
    * @return previous value if exists, null otherwise
    */
   public ExecutorFactoryConfigurationBuilder addProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
   }

   /**
    * Set key/value properties to this executor factory configuration
    *
    * @param props Properties
    * @return this ExecutorFactoryConfig
    */
   public ExecutorFactoryConfigurationBuilder withProperties(Properties props) {
      this.properties = props;
      return this;
   }
   
   void valididate() {
      // No-op, no validation required
   } 
   
   @Override
   ExecutorFactoryConfiguration create() {
      return new ExecutorFactoryConfiguration(factory, TypedProperties.toTypedProperties(properties));
   }
   
}