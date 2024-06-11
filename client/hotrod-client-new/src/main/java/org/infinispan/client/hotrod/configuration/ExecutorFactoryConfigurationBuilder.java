package org.infinispan.client.hotrod.configuration;

import static org.infinispan.client.hotrod.configuration.ExecutorFactoryConfiguration.FACTORY;
import static org.infinispan.client.hotrod.configuration.ExecutorFactoryConfiguration.FACTORY_CLASS;
import static org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration.PROPERTIES;

import java.util.Properties;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
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
   private final AttributeSet attributes;

   ExecutorFactoryConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.attributes = ExecutorFactoryConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Specify factory class for executor
    *
    * @param factoryClass
    *           clazz
    * @return this ExecutorFactoryConfig
    */
   public ExecutorFactoryConfigurationBuilder factoryClass(Class<? extends ExecutorFactory> factoryClass) {
      attributes.attribute(FACTORY_CLASS).set(factoryClass);
      return this;
   }

   public ExecutorFactoryConfigurationBuilder factoryClass(String factoryClass) {
      return factoryClass(Util.loadClass(factoryClass, builder.classLoader()));
   }

   public ExecutorFactoryConfigurationBuilder factory(ExecutorFactory factory) {
      attributes.attribute(FACTORY).set(factory);
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
      attributes.attribute(PROPERTIES).get().put(key, value);
      return this;
   }

   /**
    * Set key/value properties to this executor factory configuration
    *
    * @param props Properties
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
   public ExecutorFactoryConfiguration create() {
      return new ExecutorFactoryConfiguration(attributes.protect());
   }

   @Override
   public ExecutorFactoryConfigurationBuilder read(ExecutorFactoryConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "ExecutorFactoryConfigurationBuilder [attributes=" + attributes + "]";
   }
}
