package org.horizon.test.fwk;

import org.horizon.config.Configuration;
import org.horizon.config.GlobalConfiguration;
import org.horizon.manager.CacheManager;
import org.horizon.manager.DefaultCacheManager;
import org.horizon.remoting.transport.jgroups.JGroupsTransport;

import java.util.Properties;

/**
 * CacheManagers in unit tests should be created with this factory, in order to avoit resource clashes.
 *
 * @author Mircea.Markus@jboss.com
 */
public class TestCacheManagerFactory {

   /**
    * Creates an cache manager that does not support clustering.
    */
   public static CacheManager createLocalCacheManager() {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
//      amendJmx(globalConfiguration);
      return new DefaultCacheManager(globalConfiguration);
   }

   /**
    * Creates an cache manager that does support clustering.
    */
   public static CacheManager createClusteredCacheManager() {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
//      amendJmx(globalConfiguration);
      Properties newTransportProps = new Properties();
      newTransportProps.put(JGroupsTransport.CONFIGURATION_STRING, JGroupsConfigBuilder.getJGroupsConfig());
      globalConfiguration.setTransportProperties(newTransportProps);
      return new DefaultCacheManager(globalConfiguration);
   }

   /**
    * Creates a cache manager and ammends the supplied configuration in order to avoid conflicts (e.g. jmx, jgroups)
    * during running tests in parallel.
    */
   public static CacheManager createCacheManager(GlobalConfiguration configuration) {
      amendTransport(configuration);
//      amendJmx(configuration);
      return new DefaultCacheManager(configuration);
   }

   /**
    * Creates a local cache manager and ammends so that it won't conflict (e.g. jmx) with other managers whilst running
    * tests in parallel.
    */
   public static CacheManager createCacheManager(Configuration defaultCacheConfig) {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
//      amendJmx(globalConfiguration);
      return new DefaultCacheManager(globalConfiguration, defaultCacheConfig);
   }

   public static DefaultCacheManager createCacheManager(GlobalConfiguration configuration, Configuration defaultCfg) {
      amendTransport(configuration);
//      amendJmx(configuration);
      return new DefaultCacheManager(configuration, defaultCfg);
   }

   private static void amendJmx(GlobalConfiguration globalConfiguration) {
      globalConfiguration.setExposeGlobalJmxStatistics(false);
      globalConfiguration.setAllowDuplicateDomains(true);
   }

   private static void amendTransport(GlobalConfiguration configuration) {
      if (configuration.getTransportClass() != null) { //this is local
         Properties newTransportProps = new Properties();
         newTransportProps.put(JGroupsTransport.CONFIGURATION_STRING, JGroupsConfigBuilder.getJGroupsConfig());
         configuration.setTransportProperties(newTransportProps);
      }
   }
}
