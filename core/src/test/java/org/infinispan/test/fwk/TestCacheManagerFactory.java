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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CacheManagers in unit tests should be created with this factory, in order to avoid resource clashes.
 * See http://community.jboss.org/wiki/ParallelTestSuite for more details.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
public class TestCacheManagerFactory {


   private static AtomicInteger jmxDomainPostfix = new AtomicInteger();

   public static final String MARSHALLER = System.getProperties().getProperty("infinispan.marshaller.class");
   private static Log log = LogFactory.getLog(TestCacheManagerFactory.class);

   private static ThreadLocal<PerThreadCacheManagers> perThreadCacheManagers = new ThreadLocal<PerThreadCacheManagers>() {
      @Override
      protected PerThreadCacheManagers initialValue() {
         return new PerThreadCacheManagers();
      }
   };

   private static DefaultCacheManager newDefaultCacheManager(GlobalConfiguration gc, Configuration c, boolean keepJmxDomain) {
      if (!keepJmxDomain) {
         gc.setJmxDomain("infinispan" + jmxDomainPostfix.incrementAndGet());
      }
      return newDefaultCacheManager(gc, c);
   }

   public static CacheManager fromXml(String xmlFile, boolean allowDupeDomains) throws IOException {
      InfinispanConfiguration parser = InfinispanConfiguration.newInfinispanConfiguration(
               xmlFile,
               InfinispanConfiguration.resolveSchemaPath(),
               new ConfigurationValidatingVisitor());
      return fromConfigFileParser(parser, allowDupeDomains);
   }

   public static CacheManager fromXml(String xmlFile) throws IOException {
      return fromXml(xmlFile, false);
   }

   public static CacheManager fromStream(InputStream is) throws IOException {
      return fromStream(is, false);
   }

   public static CacheManager fromStream(InputStream is, boolean allowDupeDomains) throws IOException {
      InfinispanConfiguration parser = InfinispanConfiguration.newInfinispanConfiguration(
               is, InfinispanConfiguration.findSchemaInputStream(),
               new ConfigurationValidatingVisitor());
      return fromConfigFileParser(parser, allowDupeDomains);
   }

   private static CacheManager fromConfigFileParser(InfinispanConfiguration parser, boolean allowDupeDomains) {
      GlobalConfiguration gc = parser.parseGlobalConfiguration();
      if (allowDupeDomains) gc.setAllowDuplicateDomains(true);
      Map<String, Configuration> named = parser.parseNamedConfigurations();
      Configuration c = parser.parseDefaultConfiguration();

      minimizeThreads(gc);

      CacheManager cm = newDefaultCacheManager(gc, c, false);
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
      return newDefaultCacheManager(globalConfiguration, c, false);
   }

   private static void amendJTA(Configuration c) {
      c.setTransactionManagerLookupClass(TransactionSetup.getManagerLookup());
   }

   /**
    * Creates an cache manager that does support clustering.
    */
   public static CacheManager createClusteredCacheManager() {
      return createClusteredCacheManager(new Configuration());
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
      return newDefaultCacheManager(globalConfiguration, defaultCacheConfig, false);
   }

   /**
    * Creates a cache manager and amends the supplied configuration in order to avoid conflicts (e.g. jmx, jgroups)
    * during running tests in parallel.
    */
   public static CacheManager createCacheManager(GlobalConfiguration configuration) {
      return internalCreateJmxDomain(configuration, false);
   }

   /**
    * Creates a cache manager that won't try to modify the configured jmx domain name: {@link org.infinispan.config.GlobalConfiguration#getJmxDomain()}}.
    * This method must be used with care, and one should make sure that no domain name collision happens when the parallel suite executes.
    * An approach to ensure this, is to set the domain name to the name of the test class that instantiates the CacheManager.
    */
   public static CacheManager createCacheManagerEnforceJmxDomain(GlobalConfiguration configuration) {
      return internalCreateJmxDomain(configuration, true);
   }

   private static CacheManager internalCreateJmxDomain(GlobalConfiguration configuration, boolean enforceJmxDomain) {
      amendMarshaller(configuration);
      minimizeThreads(configuration);
      amendTransport(configuration);
      return newDefaultCacheManager(configuration, new Configuration(), enforceJmxDomain);
   }

   /**
    * Creates a local cache manager and amends so that it won't conflict (e.g. jmx) with other managers whilst running
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
      GlobalConfiguration globalConfiguration;
      if (defaultCacheConfig.getCacheMode().isClustered())
         globalConfiguration = GlobalConfiguration.getClusteredDefault();
      else
         globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      amendMarshaller(globalConfiguration);
      minimizeThreads(globalConfiguration);
      if (transactional) amendJTA(defaultCacheConfig);
      return newDefaultCacheManager(globalConfiguration, defaultCacheConfig, false);
   }

   public static CacheManager createCacheManager(GlobalConfiguration configuration, Configuration defaultCfg) {
      return createCacheManager(configuration, defaultCfg, false, false);
   }

   public static CacheManager createCacheManager(GlobalConfiguration configuration, Configuration defaultCfg, boolean transactional) {
      minimizeThreads(configuration);
      amendMarshaller(configuration);
      amendTransport(configuration);
      if (transactional) amendJTA(defaultCfg);
      return newDefaultCacheManager(configuration, defaultCfg, false);
   }

   private static CacheManager createCacheManager(GlobalConfiguration configuration, Configuration defaultCfg, boolean transactional, boolean keepJmxDomainName) {
      return createCacheManager(configuration, defaultCfg, transactional, keepJmxDomainName, false);
   }

   public static CacheManager createCacheManager(GlobalConfiguration configuration, Configuration defaultCfg, boolean transactional, boolean keepJmxDomainName, boolean dontFixTransport) {
      minimizeThreads(configuration);
      amendMarshaller(configuration);
      if (!dontFixTransport) amendTransport(configuration);
      if (transactional) amendJTA(defaultCfg);
      return newDefaultCacheManager(configuration, defaultCfg, keepJmxDomainName);
   }

   /**
    * @see #createCacheManagerEnforceJmxDomain(String)
    */
   public static CacheManager createCacheManagerEnforceJmxDomain(String jmxDomain) {
      return createCacheManagerEnforceJmxDomain(jmxDomain, true, true);
   }

   /**
    * @see #createCacheManagerEnforceJmxDomain(String)
    */
   public static CacheManager createCacheManagerEnforceJmxDomain(String jmxDomain, boolean exposeGlobalJmx, boolean exposeCacheJmx) {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      globalConfiguration.setJmxDomain(jmxDomain);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration.setExposeGlobalJmxStatistics(exposeGlobalJmx);
      Configuration configuration = new Configuration();
      configuration.setExposeJmxStatistics(exposeCacheJmx);
      return createCacheManager(globalConfiguration, configuration, false, true);
   }

   public static Configuration getDefaultConfiguration(boolean transactional) {
      Configuration c = new Configuration();
      if (transactional) amendJTA(c);
      return c;
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

   public static void minimizeThreads(GlobalConfiguration gc) {
      Properties p = new Properties();
      p.setProperty("maxThreads", "1");
      gc.setAsyncTransportExecutorProperties(p);
   }

   public static void amendMarshaller(GlobalConfiguration configuration) {
      if (MARSHALLER != null) {
         try {
            Util.loadClass(MARSHALLER);
            configuration.setMarshallerClass(MARSHALLER);
         } catch (ClassNotFoundException e) {
            // No-op, stick to GlobalConfiguration default.
         }
      }
   }

   private static DefaultCacheManager newDefaultCacheManager(GlobalConfiguration gc, Configuration c) {
      DefaultCacheManager defaultCacheManager = new DefaultCacheManager(gc, c, true);
      PerThreadCacheManagers threadCacheManagers = perThreadCacheManagers.get();
      String methodName = extractMethodName();
      threadCacheManagers.add(methodName, defaultCacheManager);
      return defaultCacheManager;
   }

   private static String extractMethodName() {
      StackTraceElement[] stack = Thread.currentThread().getStackTrace();
      if (stack.length == 0) return null;
      for (int i = stack.length - 1; i > 0; i--)
      {
         StackTraceElement e = stack[i];
         String className = e.getClassName();
         if ((className.indexOf("org.infinispan") != -1) && className.indexOf("org.infinispan.test") < 0)
            return e.toString();
      }
      return null;
   }

   static void testFinished(String testName) {
      perThreadCacheManagers.get().checkManagersClosed(testName);
   }

   private static class PerThreadCacheManagers {
      HashMap<String, CacheManager> cacheManagers = new HashMap<String, CacheManager>();

      public void checkManagersClosed(String testName) {
         for (String cmName : cacheManagers.keySet()) {
            CacheManager cm = cacheManagers.get(cmName);
            if (cm.getStatus().allowInvocations()) {
               String thName = Thread.currentThread().getName();
               String errorMessage = '\n' +
                     "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n" +
                     "!!!!!! (" + thName + ") Exiting because " + testName + " has NOT shut down all the cache managers it has started !!!!!!!\n" +
                     "!!!!!! (" + thName + ") The still-running cacheManager was created here: " + cmName + " !!!!!!!\n" +
                     "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
               log.error(errorMessage);
               System.err.println(errorMessage);
               System.exit(9);
            }
         }
         cacheManagers.clear();
      }

      public void add(String methodName, DefaultCacheManager cm) {
         cacheManagers.put(methodName, cm);
      }
   }
}
