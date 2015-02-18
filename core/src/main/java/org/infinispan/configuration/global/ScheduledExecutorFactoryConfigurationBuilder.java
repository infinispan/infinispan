package org.infinispan.configuration.global;

import java.util.Properties;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.executors.ScheduledExecutorFactory;

import static org.infinispan.configuration.global.ScheduledExecutorFactoryConfiguration.*;
/**
 * Configures executor factory.
 */
public class ScheduledExecutorFactoryConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<ScheduledExecutorFactoryConfiguration> {
   private final AttributeSet attributes;

   ScheduledExecutorFactoryConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = ScheduledExecutorFactoryConfiguration.attributeDefinitionSet();
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
      attributes.attribute(FACTORY).set(factory);
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
      attributes.attribute(PROPERTIES).get().put(key, value);
      return this;
   }

   /**
    * Set key/value properties to this executor factory configuration
    *
    * @param props Properties
    * @return this ScheduledExecutorFactoryConfig
    */
   public ScheduledExecutorFactoryConfigurationBuilder withProperties(Properties props) {
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(props));
      return this;
   }

   @Override
   public
   void validate() {
      // No-op, no validation required
   }

   @Override
   public
   ScheduledExecutorFactoryConfiguration create() {
      return new ScheduledExecutorFactoryConfiguration(attributes);
   }

   @Override
   public ScheduledExecutorFactoryConfigurationBuilder read(ScheduledExecutorFactoryConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "ScheduledExecutorFactoryConfigurationBuilder [attributes=" + attributes + "]";
   }
}
