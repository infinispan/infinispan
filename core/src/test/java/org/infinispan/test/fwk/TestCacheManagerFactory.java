package org.infinispan.test.fwk;

import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationValidatingVisitor;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.config.InfinispanConfiguration;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
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

   private static DefaultCacheManager newDefaultCacheManager(GlobalConfiguration gc, Configuration c) {
      return new DefaultCacheManager(gc, c);
   }

   public static CacheManager fromXml(String xmlFile) throws IOException {
      InfinispanConfiguration parser = InfinispanConfiguration.newInfinispanConfiguration(
               xmlFile,
               InfinispanConfiguration.resolveSchemaPath(),
               new ConfigurationValidatingVisitor());
      return fromConfigFileParser(parser);
   }

   public static CacheManager fromStream(InputStream is) throws IOException {
      InfinispanConfiguration parser = InfinispanConfiguration.newInfinispanConfiguration(
               is, InfinispanConfiguration.findSchemaInputStream(),
               new ConfigurationValidatingVisitor());
      return fromConfigFileParser(parser);      
   }

   private static CacheManager fromConfigFileParser(InfinispanConfiguration parser) {
      GlobalConfiguration gc = parser.parseGlobalConfiguration();
      Map<String, Configuration> named = parser.parseNamedConfigurations();
      Configuration c = parser.parseDefaultConfiguration();

      minimizeThreads(gc);

      CacheManager cm = new DefaultCacheManager(gc, c, false);
      for (Map.Entry<String, Configuration> e: named.entrySet()) cm.defineConfiguration(e.getKey(), e.getValue());
      cm.start();
      return cm;
   }

   /**
    * Creates an cache manager that does not support clustering or transactions.
    */
   public static CacheManager createLocalCacheManager() {
      return createLocalCacheManager(false);
   }

   private static void minimizeThreads(GlobalConfiguration gc) {
      Properties p = new Properties();
      p.setProperty("maxThreads", "1");
      gc.setAsyncTransportExecutorProperties(p);
   }

   /**
    * Creates an cache manager that does not support clustering.
    *
    * @param transactional if true, the cache manager will support transactions by default.
    */
   public static CacheManager createLocalCacheManager(boolean transactional) {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      amendMarshaller(globalConfiguration);
      minimizeThreads(globalConfiguration);
      Configuration c = new Configuration();
      if (transactional) amendJTA(c);
      return newDefaultCacheManager(globalConfiguration, c);
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
      minimizeThreads(globalConfiguration);
      Properties newTransportProps = new Properties();
      newTransportProps.put(JGroupsTransport.CONFIGURATION_STRING, JGroupsConfigBuilder.getJGroupsConfig());
      globalConfiguration.setTransportProperties(newTransportProps);
      return newDefaultCacheManager(globalConfiguration, new Configuration());
   }

   /**
    * Creates an cache manager that does support clustering with a given default cache configuration.
    */
   public static CacheManager createClusteredCacheManager(Configuration defaultCacheConfig) {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      amendMarshaller(globalConfiguration);
      minimizeThreads(globalConfiguration);
      Properties newTransportProps = new Properties();
      newTransportProps.put(JGroupsTransport.CONFIGURATION_STRING, JGroupsConfigBuilder.getJGroupsConfig());
      globalConfiguration.setTransportProperties(newTransportProps);
      return newDefaultCacheManager(globalConfiguration, defaultCacheConfig);
   }

   /**
    * Creates a cache manager and ammends the supplied configuration in order to avoid conflicts (e.g. jmx, jgroups)
    * during running tests in parallel.
    */
   public static CacheManager createCacheManager(GlobalConfiguration configuration) {
      amendMarshaller(configuration);
      minimizeThreads(configuration);
      amendTransport(configuration);
      return newDefaultCacheManager(configuration, new Configuration());
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
      minimizeThreads(globalConfiguration);
      if (transactional) amendJTA(defaultCacheConfig);
      return newDefaultCacheManager(globalConfiguration, defaultCacheConfig);
   }

   public static CacheManager createCacheManager(GlobalConfiguration configuration, Configuration defaultCfg) {
      return createCacheManager(configuration, defaultCfg, false);
   }

   public static CacheManager createCacheManager(GlobalConfiguration configuration, Configuration defaultCfg, boolean transactional) {
      minimizeThreads(configuration);
      amendMarshaller(configuration);
      amendTransport(configuration);
      if (transactional) amendJTA(defaultCfg);
      return newDefaultCacheManager(configuration, defaultCfg);
   }

   public static CacheManager createJmxEnabledCacheManager(String jmxDomain) {
      return createJmxEnabledCacheManager(jmxDomain, true, true);
   }

   public static CacheManager createJmxEnabledCacheManager(String jmxDomain, boolean exposeGlobalJmx, boolean exposeCacheJmx) {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      globalConfiguration.setJmxDomain(jmxDomain);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration.setExposeGlobalJmxStatistics(exposeGlobalJmx);
      Configuration configuration = new Configuration();
      configuration.setExposeJmxStatistics(exposeCacheJmx);
      return createCacheManager(globalConfiguration, configuration);
   }

   private static void amendTransport(GlobalConfiguration configuration) {
      if (configuration.getTransportClass() != null) { //this is local
         Properties newTransportProps = new Properties();
         Properties previousSettings = configuration.getTransportProperties();
         if (previousSettings!=null) {
            newTransportProps.putAll(previousSettings);
         }
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
