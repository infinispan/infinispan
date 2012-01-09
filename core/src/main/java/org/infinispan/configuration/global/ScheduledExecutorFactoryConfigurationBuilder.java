package org.infinispan.configuration.global;

import java.util.Properties;

import org.infinispan.executors.DefaultScheduledExecutorFactory;
import org.infinispan.executors.ScheduledExecutorFactory;
import org.infinispan.util.TypedProperties;

/**
 * Configures executor factory.
 */
public class ScheduledExecutorFactoryConfigurationBuilder extends AbstractGlobalConfigurationBuilder<ScheduledExecutorFactoryConfiguration> {
   
   private ScheduledExecutorFactory factory = new DefaultScheduledExecutorFactory();
   private Properties properties;
   
   ScheduledExecutorFactoryConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      this.properties = new Properties();
   }
   
   /**
    * Specify factory class for executor
    * 
    * NOTE: Currently Infinispan will not use the object instance, but instead instantiate a new
    * instance of the class. Therefore, do not expect any state to survive, and provide a no-args
    * constructor to any instance. This will be resolved in Infinispan 5.2.0
    *
    * @param factory clazz
    * @return this ScheduledExecutorFactoryConfig
    */
   public ScheduledExecutorFactoryConfigurationBuilder factory(ScheduledExecutorFactory factory) {
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
   public ScheduledExecutorFactoryConfigurationBuilder addProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
   }

   /**
    * Set key/value properties to this executor factory configuration
    *
    * @param props Properties
    * @return this ScheduledExecutorFactoryConfig
    */
   public ScheduledExecutorFactoryConfigurationBuilder withProperties(Properties props) {
      this.properties = props;
      return this;
   }
   
   void valididate() {
      // No-op, no validation required
   } 
   
   @Override
   ScheduledExecutorFactoryConfiguration create() {
      return new ScheduledExecutorFactoryConfiguration(factory, TypedProperties.toTypedProperties(properties));
   }
   
   @Override
   public ScheduledExecutorFactoryConfigurationBuilder read(ScheduledExecutorFactoryConfiguration template) {
      this.factory = template.factory();
      this.properties = template.properties();
      
      return this;
   }
   
}