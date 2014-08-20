package org.infinispan.persistence.remote.configuration;

import java.util.Properties;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.executors.DefaultExecutorFactory;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.commons.util.TypedProperties;

/**
 * Configures executor factory.
 */
public class ExecutorFactoryConfigurationBuilder extends AbstractRemoteStoreConfigurationChildBuilder<RemoteStoreConfigurationBuilder> implements Builder<ExecutorFactoryConfiguration> {

   private ExecutorFactory factory = new DefaultExecutorFactory();
   private Properties properties;

   ExecutorFactoryConfigurationBuilder(RemoteStoreConfigurationBuilder builder) {
      super(builder);
      this.properties = new Properties();
   }

   /**
    * Specify factory class for executor
    *
    * NOTE: Currently Infinispan will not use the object instance, but instead instantiate a new
    * instance of the class. Therefore, do not expect any state to survive, and provide a no-args
    * constructor to any instance. This will be resolved in Infinispan 5.2.0
    *
    * @param factory
    *           clazz
    * @return this ExecutorFactoryConfig
    */
   public ExecutorFactoryConfigurationBuilder factory(ExecutorFactory factory) {
      this.factory = factory;
      return this;
   }

   /**
    * Add key/value property pair to this executor factory configuration
    *
    * @param key
    *           property key
    * @param value
    *           property value
    * @return previous value if exists, null otherwise
    */
   public ExecutorFactoryConfigurationBuilder addExecutorProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
   }

   /**
    * Set key/value properties to this executor factory configuration
    *
    * @param props
    *           Properties
    * @return this ExecutorFactoryConfig
    */
   public ExecutorFactoryConfigurationBuilder withExecutorProperties(Properties props) {
      this.properties = props;
      return this;
   }

   @Override
   public void validate() {
      // No-op, no validation required
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public ExecutorFactoryConfiguration create() {
      return new ExecutorFactoryConfiguration(factory, TypedProperties.toTypedProperties(properties));
   }

   @Override
   public ExecutorFactoryConfigurationBuilder read(ExecutorFactoryConfiguration template) {
      this.factory = template.factory();
      this.properties = template.properties();

      return this;
   }

   @Override
   public String toString() {
      return "ExecutorFactoryConfigurationBuilder{" + "factory=" + factory + ", properties=" + properties + '}';
   }
}