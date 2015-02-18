package org.infinispan.persistence.remote.configuration;

import static org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration.PROPERTIES;
import static org.infinispan.persistence.remote.configuration.ExecutorFactoryConfiguration.EXECUTOR_FACTORY;

import java.util.Properties;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * Configures executor factory.
 */
public class ExecutorFactoryConfigurationBuilder extends AbstractRemoteStoreConfigurationChildBuilder<RemoteStoreConfigurationBuilder> implements Builder<ExecutorFactoryConfiguration> {
   private final AttributeSet attributes;
   ExecutorFactoryConfigurationBuilder(RemoteStoreConfigurationBuilder builder) {
      super(builder);
      attributes = ExecutorFactoryConfiguration.attributeSet();
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
      attributes.attribute(EXECUTOR_FACTORY).set(factory);
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
      TypedProperties properties = attributes.attribute(PROPERTIES).get();
      properties.put(key, value);
      attributes.attribute(PROPERTIES).set(properties);
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
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(props));
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
      return new ExecutorFactoryConfiguration(attributes.protect());
   }

   @Override
   public ExecutorFactoryConfigurationBuilder read(ExecutorFactoryConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "ExecutorFactoryConfigurationBuilder [attributes=" + attributes + "]";
   }
}
