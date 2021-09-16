package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.GlobalJmxConfiguration.DOMAIN;
import static org.infinispan.configuration.global.GlobalJmxConfiguration.ENABLED;
import static org.infinispan.configuration.global.GlobalJmxConfiguration.MBEAN_SERVER_LOOKUP;
import static org.infinispan.configuration.global.GlobalJmxConfiguration.PROPERTIES;

import java.util.Properties;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.PlatformMBeanServerLookup;
import org.infinispan.commons.util.TypedProperties;

/**
 * Configures JMX for the cache manager and its caches.
 *
 * @since 10.1.3
 */
public class GlobalJmxConfigurationBuilder extends GlobalJmxStatisticsConfigurationBuilder implements Builder<GlobalJmxConfiguration> {

   private final AttributeSet attributes;

   GlobalJmxConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = GlobalJmxConfiguration.attributeDefinitionSet();
   }

   /**
    * Sets properties which are then passed to the MBean Server Lookup implementation specified.
    *
    * @param properties properties to pass to the MBean Server Lookup
    */
   public GlobalJmxConfigurationBuilder withProperties(Properties properties) {
      attributes.attribute(PROPERTIES).set(new TypedProperties(properties));
      return this;
   }

   public GlobalJmxConfigurationBuilder addProperty(String key, String value) {
      TypedProperties properties = attributes.attribute(PROPERTIES).get();
      properties.put(key, value);
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(properties));
      return this;
   }

   /**
    * If JMX is enabled then all 'published' JMX objects will appear under this name. This is optional, if not specified
    * a default domain name will be set by default.
    *
    * @param domain
    */
   public GlobalJmxConfigurationBuilder domain(String domain) {
      attributes.attribute(DOMAIN).set(domain);
      return this;
   }

   /**
    * If JMX is enabled, this property represents the name of this cache manager. It offers the possibility for clients
    * to provide a user-defined name to the cache manager which later can be used to identify the cache manager within a
    * JMX based management tool amongst other cache managers that might be running under the same JVM.
    *
    * @deprecated Use {@link GlobalConfigurationBuilder#cacheManagerName(String)} instead
    */
   @Deprecated
   public GlobalJmxConfigurationBuilder cacheManagerName(String cacheManagerName) {
      getGlobalConfig().cacheContainer().name(cacheManagerName);
      return this;
   }

   /**
    * Sets the instance of the {@link org.infinispan.commons.jmx.MBeanServerLookup} class to be used to bound JMX MBeans
    * to.
    *
    * @param mBeanServerLookupInstance An instance of {@link org.infinispan.commons.jmx.MBeanServerLookup}
    */
   public GlobalJmxConfigurationBuilder mBeanServerLookup(MBeanServerLookup mBeanServerLookupInstance) {
      attributes.attribute(MBEAN_SERVER_LOOKUP).set(mBeanServerLookupInstance);
      return this;
   }

   /**
    * Disables JMX in the cache manager.
    */
   public GlobalJmxConfigurationBuilder disable() {
      enabled(false);
      return this;
   }

   /**
    * Enables JMX in the cache manager.
    */
   public GlobalJmxConfigurationBuilder enable() {
      enabled(true);
      return this;
   }

   /**
    * Enables JMX in the cache manager.
    */
   public GlobalJmxConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   @Override
   public GlobalJmxConfiguration create() {
      if (enabled() && attributes.attribute(MBEAN_SERVER_LOOKUP).isNull()) {
         mBeanServerLookup(new PlatformMBeanServerLookup());
      }
      return new GlobalJmxConfiguration(attributes.protect(), getGlobalConfig().cacheContainer().name());
   }

   @Override
   public GlobalJmxConfigurationBuilder read(GlobalJmxConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "GlobalJmxConfigurationBuilder [attributes=" + attributes + "]";
   }
}
