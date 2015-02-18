package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration.ALLOW_DUPLICATE_DOMAINS;
import static org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration.CACHE_MANAGER_NAME;
import static org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration.ENABLED;
import static org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration.JMX_DOMAIN;
import static org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration.MBEAN_SERVER_LOOKUP;
import static org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration.PROPERTIES;

import java.util.Properties;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.jmx.MBeanServerLookup;

/**
 * Configures whether global statistics are gathered and reported via JMX for all caches under this cache manager.
 */
public class GlobalJmxStatisticsConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<GlobalJmxStatisticsConfiguration> {
   private final AttributeSet attributes;

   GlobalJmxStatisticsConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = GlobalJmxStatisticsConfiguration.attributeDefinitionSet();
   }

   /**
    * Sets properties which are then passed to the MBean Server Lookup implementation specified.
    *
    * @param properties properties to pass to the MBean Server Lookup
    */
   public GlobalJmxStatisticsConfigurationBuilder withProperties(Properties properties) {
      attributes.attribute(PROPERTIES).set(new TypedProperties(properties));
      return this;
   }

   public GlobalJmxStatisticsConfigurationBuilder addProperty(String key, String value) {
      TypedProperties properties = attributes.attribute(PROPERTIES).get();
      properties.put(key, value);
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(properties));
      return this;
   }

   /**
    * If JMX statistics are enabled then all 'published' JMX objects will appear under this name.
    * This is optional, if not specified an object name will be created for you by default.
    *
    * @param jmxDomain
    */
   public GlobalJmxStatisticsConfigurationBuilder jmxDomain(String jmxDomain) {
      attributes.attribute(JMX_DOMAIN).set(jmxDomain);
      return this;
   }

   /**
    * If true, multiple cache manager instances could be configured under the same configured JMX
    * domain. Each cache manager will in practice use a different JMX domain that has been
    * calculated based on the configured one by adding an incrementing index to it.
    *
    * @param allowDuplicateDomains
    */
   public GlobalJmxStatisticsConfigurationBuilder allowDuplicateDomains(Boolean allowDuplicateDomains) {
      attributes.attribute(ALLOW_DUPLICATE_DOMAINS).set(allowDuplicateDomains);
      return this;
   }

   /**
    * If JMX statistics are enabled, this property represents the name of this cache manager. It
    * offers the possibility for clients to provide a user-defined name to the cache manager
    * which later can be used to identify the cache manager within a JMX based management tool
    * amongst other cache managers that might be running under the same JVM.
    *
    * @param cacheManagerName
    */
   public GlobalJmxStatisticsConfigurationBuilder cacheManagerName(String cacheManagerName) {
      attributes.attribute(CACHE_MANAGER_NAME).set(cacheManagerName);
      return this;
   }

   /**
    * Sets the instance of the {@link org.infinispan.jmx.MBeanServerLookup} class to be used to bound JMX MBeans to.
    *
    * @param mBeanServerLookupInstance An instance of {@link org.infinispan.jmx.MBeanServerLookup}
    */
   public GlobalJmxStatisticsConfigurationBuilder mBeanServerLookup(MBeanServerLookup mBeanServerLookupInstance) {
      attributes.attribute(MBEAN_SERVER_LOOKUP).set(mBeanServerLookupInstance);
      return this;
   }

   public GlobalJmxStatisticsConfigurationBuilder disable() {
      enabled(false);
      return this;
   }

   public GlobalJmxStatisticsConfigurationBuilder enable() {
      enabled(true);
      return this;
   }

   public GlobalJmxStatisticsConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   @Override
   public
   void validate() {
      // No-op, no validation required
   }

   @Override
   public
   GlobalJmxStatisticsConfiguration create() {
      return new GlobalJmxStatisticsConfiguration(attributes.protect());
   }

   @Override
   public
   GlobalJmxStatisticsConfigurationBuilder read(GlobalJmxStatisticsConfiguration template) {
      attributes.read(template.attributes());

      return this;
   }

   @Override
   public String toString() {
      return "GlobalJmxStatisticsConfigurationBuilder [attributes=" + attributes + "]";
   }

}