package org.infinispan.client.hotrod.configuration;

import java.util.Properties;

import org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;

/**
 * Configures executor factory.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class ExecutorFactoryConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<ExecutorFactoryConfiguration> {

   private Class<? extends ExecutorFactory> factoryClass = DefaultAsyncExecutorFactory.class;
   private ExecutorFactory factory;
   private Properties properties;
   private final ConfigurationBuilder builder;

   ExecutorFactoryConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.builder = builder;
      this.properties = new Properties();
   }

   /**
    * Specify factory class for executor
    *
    * @param factoryClass
    *           clazz
    * @return this ExecutorFactoryConfig
    */
   public ExecutorFactoryConfigurationBuilder factoryClass(Class<? extends ExecutorFactory> factoryClass) {
      this.factoryClass = factoryClass;
      return this;
   }

   public ExecutorFactoryConfigurationBuilder factoryClass(String factoryClass) {
      this.factoryClass = Util.loadClass(factoryClass, builder.classLoader());
      return this;
   }

   /**
    * Specify factory class for executor
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
   }

   @Override
   public ExecutorFactoryConfiguration create() {
      if (factory != null)
         return new ExecutorFactoryConfiguration(factory, TypedProperties.toTypedProperties(properties));
      else
         return new ExecutorFactoryConfiguration(factoryClass, TypedProperties.toTypedProperties(properties));
   }

   @Override
   public ExecutorFactoryConfigurationBuilder read(ExecutorFactoryConfiguration template) {
      this.factory = template.factory();
      this.factoryClass = template.factoryClass();
      this.properties = template.properties();

      return this;
   }

   @Override
   public String toString() {
      return "ExecutorFactoryConfigurationBuilder [factoryClass=" + factoryClass + ", factory=" + factory + ", properties=" + properties + "]";
   }
}
