package org.infinispan.configuration.global;

import java.util.Properties;

import org.infinispan.commons.jmx.MBeanServerLookup;

/**
 * Configures JMX for the cache manager and its caches.
 *
 * @deprecated since 10.1.3. Use {@link GlobalJmxConfigurationBuilder} instead. This will be removed in next major version.
 */
@Deprecated
public abstract class GlobalJmxStatisticsConfigurationBuilder extends AbstractGlobalConfigurationBuilder {

   GlobalJmxStatisticsConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
   }

   /**
    * Sets properties which are then passed to the MBean Server Lookup implementation specified.
    *
    * @param properties properties to pass to the MBean Server Lookup
    */
   public abstract GlobalJmxStatisticsConfigurationBuilder withProperties(Properties properties);

   public abstract GlobalJmxStatisticsConfigurationBuilder addProperty(String key, String value);

   /**
    * If JMX is enabled then all 'published' JMX objects will appear under this name.
    * This is optional, if not specified a default domain name will be set by default.
    *
    * @param domain
    */
   public abstract GlobalJmxStatisticsConfigurationBuilder domain(String domain);

   /**
    * If JMX is enabled then all 'published' JMX objects will appear under this name.
    * This is optional, if not specified a default domain name will be set by default.
    *
    * @deprecated Since 10.1.3, please use {@link #domain(String)} instead.
    */
   @Deprecated
   public GlobalJmxStatisticsConfigurationBuilder jmxDomain(String domain) {
      return domain(domain);
   }

   /**
    * If JMX statistics are enabled, this property represents the name of this cache manager. It
    * offers the possibility for clients to provide a user-defined name to the cache manager
    * which later can be used to identify the cache manager within a JMX based management tool
    * amongst other cache managers that might be running under the same JVM.
    *
    * @deprecated Use {@link GlobalConfigurationBuilder#cacheManagerName(String)} instead
    */
   @Deprecated
   public abstract GlobalJmxStatisticsConfigurationBuilder cacheManagerName(String cacheManagerName);

   /**
    * Sets the instance of the {@link org.infinispan.commons.jmx.MBeanServerLookup} class to be used to bound JMX MBeans to.
    *
    * @param mBeanServerLookupInstance An instance of {@link org.infinispan.commons.jmx.MBeanServerLookup}
    */
   public abstract GlobalJmxStatisticsConfigurationBuilder mBeanServerLookup(MBeanServerLookup mBeanServerLookupInstance);

   /**
    * Disables JMX in the cache manager.
    */
   public abstract GlobalJmxStatisticsConfigurationBuilder disable();

   /**
    * Enables JMX in the cache manager.
    */
   public abstract GlobalJmxStatisticsConfigurationBuilder enable();

   /**
    * Enables JMX in the cache manager.
    */
   public abstract GlobalJmxStatisticsConfigurationBuilder enabled(boolean enabled);
}
