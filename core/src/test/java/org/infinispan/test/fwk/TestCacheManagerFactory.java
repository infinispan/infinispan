package org.infinispan.test.fwk;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Properties;

/**
 * CacheManagers in unit tests should be created with this factory, in order to avoit resource clashes.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
public class TestCacheManagerFactory {

   public static final String MARSHALLER = System.getProperties().getProperty("infinispan.marshaller.class");
   private static Log log = LogFactory.getLog(TestCacheManagerFactory.class);

   /**
    * Creates an cache manager that does not support clustering or transactions.
    */
   public static CacheManager createLocalCacheManager() {
      return createLocalCacheManager(false);
   }

   /**
    * Creates an cache manager that does not support clustering.
    *
    * @param transactional if true, the cache manager will support transactions by default.
    */
   public static CacheManager createLocalCacheManager(boolean transactional) {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      amendMarshaller(globalConfiguration);
      Configuration c = new Configuration();
      if (transactional) amendJTA(c);
      return new DefaultCacheManager(globalConfiguration, c);
   }

   private static void amendJTA(Configuration c) {
      c.setTransactionManagerLookupClass(TransactionSetup.getManagerLookup());
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
    * tests in parallel.  This is a non-transactional cache manager.
    */
   public static CacheManager createCacheManager(Configuration defaultCacheConfig) {
      if (defaultCacheConfig.getTransactionManagerLookup() != null || defaultCacheConfig.getTransactionManagerLookupClass() != null) {
         log.error("You have passed in a default configuration which has transactional elements set.  If you wish to use transactions, use the TestCacheManagerFactory.createCacheManager(Configuration defaultCacheConfig, boolean transactional) method.");
      }
      defaultCacheConfig.setTransactionManagerLookup(null);
      defaultCacheConfig.setTransactionManagerLookupClass(null);
      return createCacheManager(defaultCacheConfig, false);
   }

   public static CacheManager createCacheManager(Configuration defaultCacheConfig, boolean transactional) {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      amendMarshaller(globalConfiguration);
//      amendJmx(globalConfiguration);
      if (transactional) amendJTA(defaultCacheConfig);
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

   public static Configuration getDefaultConfiguration(boolean transactional) {
      Configuration c = new Configuration();
      if (transactional) amendJTA(c);
      return c;
   }
}
