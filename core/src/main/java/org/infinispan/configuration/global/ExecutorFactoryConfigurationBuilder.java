package org.infinispan.configuration.global;

import java.util.Properties;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.commons.util.TypedProperties;

import static org.infinispan.configuration.global.ExecutorFactoryConfiguration.*;
/**
 * Configures executor factory.
 */
public class ExecutorFactoryConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<ExecutorFactoryConfiguration> {
   private final AttributeSet attributes;

   ExecutorFactoryConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      this.attributes = ExecutorFactoryConfiguration.attributeDefinitionSet();
   }

   /**
    * Specify factory class for executor
    *
    * NOTE: Currently Infinispan will not use the object instance, but instead instantiate a new
    * instance of the class. Therefore, do not expect any state to survive, and provide a no-args
    * constructor to any instance. This will be resolved in Infinispan 5.2.0
    *
    * @param factory clazz
    * @return this ExecutorFactoryConfig
    */
   public ExecutorFactoryConfigurationBuilder factory(ExecutorFactory factory) {
      attributes.attribute(FACTORY).set(factory);
      return this;
   }

   /**
    * Add key/value property pair to this executor factory configuration
    *
    * @param key   property key
    * @param value property value
    * @return this ExecutorFactoryConfig
    */
   public ExecutorFactoryConfigurationBuilder addProperty(String key, String value) {
      attributes.attribute(PROPERTIES).get().put(key, value);
      return this;
   }

   /**
    * Set key/value properties to this executor factory configuration
    *
    * @param props Properties
    * @return this ExecutorFactoryConfig
    */
   public ExecutorFactoryConfigurationBuilder withProperties(Properties props) {
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(props));
      return this;
   }

   @Override
   public void validate() {
      // No-op, no validation required
   }

   @Override
   public
   ExecutorFactoryConfiguration create() {
      return new ExecutorFactoryConfiguration(attributes);
   }

   @Override
   public
   ExecutorFactoryConfigurationBuilder read(ExecutorFactoryConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "ExecutorFactoryConfigurationBuilder [attributes=" + attributes + "]";
   }

}