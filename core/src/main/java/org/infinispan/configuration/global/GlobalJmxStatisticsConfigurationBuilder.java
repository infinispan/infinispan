package org.infinispan.configuration.global;

import java.util.Properties;

import org.infinispan.jmx.MBeanServerLookup;
import org.infinispan.jmx.PlatformMBeanServerLookup;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.Util;

/**
 * Configures whether global statistics are gathered and reported via JMX for all caches under this cache manager.
 */
public class GlobalJmxStatisticsConfigurationBuilder extends AbstractGlobalConfigurationBuilder<GlobalJmxStatisticsConfiguration> {
   
   private Properties properties = new Properties();
   private String jmxDomain = "org.infinispan";
   private Boolean allowDuplicateDomains= false;
   private String cacheManagerName = "DefaultCacheManager";
   private MBeanServerLookup mBeanServerLookupInstance = Util.getInstance(PlatformMBeanServerLookup.class);
   private boolean enabled = false;
   
   GlobalJmxStatisticsConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
   }
   
   /**
    * Sets properties which are then passed to the MBean Server Lookup implementation specified.
    *
    * @param properties properties to pass to the MBean Server Lookup
    */
   public GlobalJmxStatisticsConfigurationBuilder withProperties(Properties properties) {
      this.properties = properties;
      return this;
   }

   public GlobalJmxStatisticsConfigurationBuilder addProperty(String key, String value) {
      properties.put(key, value);
      return this;
   }

   /**
    * If JMX statistics are enabled then all 'published' JMX objects will appear under this name.
    * This is optional, if not specified an object name will be created for you by default.
    *
    * @param jmxDomain
    */
   public GlobalJmxStatisticsConfigurationBuilder jmxDomain(String jmxDomain) {
      this.jmxDomain = jmxDomain;
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
      this.allowDuplicateDomains = allowDuplicateDomains;
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
      this.cacheManagerName = cacheManagerName;
      return this;
   }

   /**
    * Sets the instance of the {@link org.infinispan.jmx.MBeanServerLookup} class to be used to bound JMX MBeans to.
    *
    * @param mBeanServerLookupInstance An instance of {@link org.infinispan.jmx.MBeanServerLookup}
    */
   public GlobalJmxStatisticsConfigurationBuilder mBeanServerLookup(MBeanServerLookup mBeanServerLookupInstance) {
      this.mBeanServerLookupInstance = mBeanServerLookupInstance;
      return this;
   }

   public GlobalJmxStatisticsConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   public GlobalJmxStatisticsConfigurationBuilder enable() {
      this.enabled = false;
      return this;
   }
   
   public GlobalJmxStatisticsConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }
   
   @Override
   void valididate() {
      // No-op, no validation required
   }
   
   @Override
   GlobalJmxStatisticsConfiguration create() {
      return new GlobalJmxStatisticsConfiguration(enabled, jmxDomain, mBeanServerLookupInstance, allowDuplicateDomains,  cacheManagerName, TypedProperties.toTypedProperties(properties));
   }
}