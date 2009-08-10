package org.infinispan.test.fwk;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.Util;

import java.util.Properties;

/**
 * CacheManagers in unit tests should be created with this factory, in order to avoit resource clashes.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
public class TestCacheManagerFactory {
   
   public static final String MARSHALLER = System.getProperties().getProperty("infinispan.marshaller.class");

   /**
    * Creates an cache manager that does not support clustering.
    */
   public static CacheManager createLocalCacheManager() {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      amendMarshaller(globalConfiguration);
//      amendJmx(globalConfiguration);
      return new DefaultCacheManager(globalConfiguration);
   }

   public static GlobalConfiguration getGlobalClusteredConfigurtion() {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      amendMarshaller(globalConfiguration);
      return globalConfiguration;
   }

   /**
    * Creates an cache manager that does support clustering.
    */
   public static CacheManager createClusteredCacheManager() {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      amendMarshaller(globalConfiguration);
//      amendJmx(globalConfiguration);
      Properties newTransportProps = new Properties();
      newTransportProps.put(JGroupsTransport.CONFIGURATION_STRING, JGroupsConfigBuilder.getJGroupsConfig());
      globalConfiguration.setTransportProperties(newTransportProps);
      return new DefaultCacheManager(globalConfiguration);
   }
   
   /**
    * Creates an cache manager that does support clustering with a given default cache configuration.
    */
   public static CacheManager createClusteredCacheManager(Configuration defaultCacheConfig) {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      amendMarshaller(globalConfiguration);
//      amendJmx(globalConfiguration);
      Properties newTransportProps = new Properties();
      newTransportProps.put(JGroupsTransport.CONFIGURATION_STRING, JGroupsConfigBuilder.getJGroupsConfig());
      globalConfiguration.setTransportProperties(newTransportProps);
      return new DefaultCacheManager(globalConfiguration, defaultCacheConfig);
   }

   /**
    * Creates a cache manager and ammends the supplied configuration in order to avoid conflicts (e.g. jmx, jgroups)
    * during running tests in parallel.
    */
   public static CacheManager createCacheManager(GlobalConfiguration configuration) {
      amendMarshaller(configuration);
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
      amendMarshaller(globalConfiguration);
//      amendJmx(globalConfiguration);
      return new DefaultCacheManager(globalConfiguration, defaultCacheConfig);
   }

   public static DefaultCacheManager createCacheManager(GlobalConfiguration configuration, Configuration defaultCfg) {
      amendMarshaller(configuration);
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
   
   private static void amendMarshaller(GlobalConfiguration configuration) {
      if (MARSHALLER != null) {
         try {
            Util.loadClass(MARSHALLER);
            configuration.setMarshallerClass(MARSHALLER);
         } catch (ClassNotFoundException e) {
            // No-op, stick to GlobalConfiguration default.
         }
      }
   }
}
