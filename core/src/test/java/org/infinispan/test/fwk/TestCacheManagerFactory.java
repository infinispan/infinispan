package org.infinispan.test.fwk;

import static org.infinispan.test.fwk.JGroupsConfigBuilder.getJGroupsConfig;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.LegacyKeySupportSystemProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.jmx.MBeanServerLookup;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * CacheManagers in unit tests should be created with this factory, in order to avoid resource clashes. See
 * http://community.jboss.org/wiki/ParallelTestSuite for more details.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
public class TestCacheManagerFactory {

   public static final int MAX_ASYNC_EXEC_THREADS = 6;
   public static final int ASYNC_EXEC_QUEUE_SIZE = 10000;
   public static final int MAX_REQ_EXEC_THREADS = 6;
   public static final int REQ_EXEC_QUEUE_SIZE = 0;
   public static final int KEEP_ALIVE = 30000;

   public static final String MARSHALLER = LegacyKeySupportSystemProperties.getProperty("infinispan.test.marshaller.class", "infinispan.marshaller.class");
   private static final Log log = LogFactory.getLog(TestCacheManagerFactory.class);

   /**
    * Note this method does not amend the global configuration to reduce overall resource footprint.  It is therefore
    * suggested to use {@link org.infinispan.test.fwk.TestCacheManagerFactory#createClusteredCacheManager(org.infinispan.configuration.cache.ConfigurationBuilder, TransportFlags)}
    * instead when this is needed
    * @param start Whether to start this cache container
    * @param gc The global configuration builder to use
    * @param c The default configuration to use
    * @param keepJmxDomain Whether or not the provided jmx domain should be used or if a unique one is generated
    * @return The resultant cache manager that is created
    */
   public static EmbeddedCacheManager newDefaultCacheManager(boolean start, GlobalConfigurationBuilder gc, ConfigurationBuilder c, boolean keepJmxDomain) {
      if (!keepJmxDomain) {
         gc.globalJmxStatistics().jmxDomain("infinispan-" + UUID.randomUUID());
      }
      return newDefaultCacheManager(start, gc, c);
   }

   private static DefaultCacheManager newDefaultCacheManager(boolean start, ConfigurationBuilderHolder holder, boolean keepJmxDomain) {
      if (!keepJmxDomain) {
         holder.getGlobalConfigurationBuilder().globalJmxStatistics().jmxDomain(
               "infinispan-" + UUID.randomUUID());
      }
      return newDefaultCacheManager(start, holder);
   }

   public static EmbeddedCacheManager fromXml(String xmlFile) throws IOException {
      return fromXml(xmlFile, false);
   }

   public static EmbeddedCacheManager fromXml(String xmlFile, boolean keepJmxDomainName) throws IOException {
      InputStream is = new FileLookup().lookupFileStrict(
            xmlFile, Thread.currentThread().getContextClassLoader());
      return fromStream(is, keepJmxDomainName);
   }

   public static EmbeddedCacheManager fromStream(InputStream is) throws IOException {
      return fromStream(is, false);
   }

   public static EmbeddedCacheManager fromStream(InputStream is, boolean keepJmxDomainName) throws IOException {
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader());
      ConfigurationBuilderHolder holder = parserRegistry.parse(is);
      return createClusteredCacheManager(holder, keepJmxDomainName);
   }

   private static void markAsTransactional(boolean transactional, ConfigurationBuilder builder) {
      if (!transactional) {
         builder.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      } else {
         builder.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL);
         //Skip this step in OSGi. This operation requires internal packages of Arjuna. These are not exported from the Arjuna
         //bundle in OSGi
         if (!Util.isOSGiContext()) {
            // Set volatile stores just in case...
            JBossTransactionsUtils.setVolatileStores();
            //automatically change default TM lookup to the desired one but only outside OSGi. In OSGi we need to use GenericTransactionManagerLookup
            builder.transaction().transactionManagerLookup((TransactionManagerLookup) Util.getInstance(TransactionSetup.getManagerLookup(), TestCacheManagerFactory.class.getClassLoader()));
         }
      }
   }

   private static void updateTransactionSupport(boolean transactional, ConfigurationBuilder builder) {
      if (transactional) amendJTA(builder);
   }

   private static void amendJTA(ConfigurationBuilder builder) {
      org.infinispan.configuration.cache.Configuration c = builder.build();
      if (c.transaction().transactionMode().equals(TransactionMode.TRANSACTIONAL) && c.transaction().transactionManagerLookup() == null) {
         builder.transaction().transactionManagerLookup((TransactionManagerLookup) Util.getInstance(TransactionSetup.getManagerLookup(), TestCacheManagerFactory.class.getClassLoader()));
      }
   }

   /**
    * Creates an cache manager that does support clustering.
    */
   public static EmbeddedCacheManager createClusteredCacheManager() {
      return createClusteredCacheManager(new ConfigurationBuilder(), new TransportFlags());
   }

   public static EmbeddedCacheManager createClusteredCacheManager(ConfigurationBuilder defaultCacheConfig, TransportFlags flags) {
      return createClusteredCacheManager(GlobalConfigurationBuilder.defaultClusteredBuilder(), defaultCacheConfig, flags);
   }

   public static EmbeddedCacheManager createClusteredCacheManager(GlobalConfigurationBuilder gcb, ConfigurationBuilder defaultCacheConfig) {
      return createClusteredCacheManager(gcb, defaultCacheConfig, new TransportFlags());
   }

   public static EmbeddedCacheManager createClusteredCacheManager(ConfigurationBuilder defaultCacheConfig) {
      return createClusteredCacheManager(GlobalConfigurationBuilder.defaultClusteredBuilder(), defaultCacheConfig);
   }

   public static EmbeddedCacheManager createClusteredCacheManager(ConfigurationBuilderHolder holder) {
      return createClusteredCacheManager(holder, false);
   }

   public static EmbeddedCacheManager createClusteredCacheManager(ConfigurationBuilderHolder holder, boolean keepJmxDomainName) {
      TransportFlags flags = new TransportFlags();
      amendGlobalConfiguration(holder.getGlobalConfigurationBuilder(), flags);
      amendJTA(holder.getDefaultConfigurationBuilder());
      for (ConfigurationBuilder builder : holder.getNamedConfigurationBuilders().values())
         amendJTA(builder);

      return newDefaultCacheManager(true, holder, keepJmxDomainName);
   }

   public static EmbeddedCacheManager createClusteredCacheManager(GlobalConfigurationBuilder gcb, ConfigurationBuilder defaultCacheConfig, TransportFlags flags) {
      return createClusteredCacheManager(gcb, defaultCacheConfig, flags, false);
   }

   public static EmbeddedCacheManager createClusteredCacheManager(GlobalConfigurationBuilder gcb,
                                                                  ConfigurationBuilder defaultCacheConfig,
                                                                  TransportFlags flags,
                                                                  boolean keepJmxDomainName) {
      amendGlobalConfiguration(gcb, flags);
      amendJTA(defaultCacheConfig);
      return newDefaultCacheManager(true, gcb, defaultCacheConfig, keepJmxDomainName);
   }

   public static void amendGlobalConfiguration(GlobalConfigurationBuilder gcb, TransportFlags flags) {
      amendMarshaller(gcb);
      minimizeThreads(gcb);
      amendTransport(gcb, flags);
   }

   public static EmbeddedCacheManager createCacheManager(ConfigurationBuilder builder) {
      return createCacheManager(new GlobalConfigurationBuilder().nonClusteredDefault(), builder);
   }

   public static EmbeddedCacheManager createCacheManager() {
      return createCacheManager(new ConfigurationBuilder());
   }

   public static EmbeddedCacheManager createCacheManager(boolean start) {
      return newDefaultCacheManager(start, new GlobalConfigurationBuilder().nonClusteredDefault(), new ConfigurationBuilder(), false);
   }

   public static EmbeddedCacheManager createCacheManager(GlobalConfigurationBuilder globalBuilder, ConfigurationBuilder builder) {
      if (globalBuilder.transport().build().transport().transport() != null) {
         throw new IllegalArgumentException("Use TestCacheManagerFactory.createClusteredCacheManager(...) for clustered cache managers");
      }
      return newDefaultCacheManager(true, globalBuilder, builder, false);
   }

   public static EmbeddedCacheManager createCacheManager(GlobalConfigurationBuilder globalBuilder, ConfigurationBuilder builder, boolean keepJmxDomain) {
      if (globalBuilder.transport().build().transport().transport() != null) {
         throw new IllegalArgumentException("Use TestCacheManagerFactory.createClusteredCacheManager(...) for clustered cache managers");
      }
      return newDefaultCacheManager(true, globalBuilder, builder, keepJmxDomain);
   }

   public static EmbeddedCacheManager createCacheManager(CacheMode mode, boolean indexing) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
         .clustering()
            .cacheMode(mode)
         .indexing()
            .index(indexing ? Index.ALL : Index.NONE)
            .addProperty("hibernate.search.lucene_version", "LUCENE_CURRENT")
         ;
      if (mode.isClustered()) {
         return createClusteredCacheManager(builder);
      }
      else {
         return createCacheManager(builder);
      }
   }

   /**
    * @see #createCacheManagerEnforceJmxDomain(String)
    */
   public static EmbeddedCacheManager createCacheManagerEnforceJmxDomain(String jmxDomain) {
      return createCacheManagerEnforceJmxDomain(jmxDomain, true, true);
   }

   public static EmbeddedCacheManager createClusteredCacheManagerEnforceJmxDomain(String jmxDomain) {
      return createClusteredCacheManagerEnforceJmxDomain(jmxDomain, true, false, new ConfigurationBuilder());
   }

   public static EmbeddedCacheManager createClusteredCacheManagerEnforceJmxDomain(String jmxDomain, boolean allowDuplicateDomains) {
      return createClusteredCacheManagerEnforceJmxDomain(jmxDomain, true, allowDuplicateDomains, new ConfigurationBuilder());
   }

   public static EmbeddedCacheManager createClusteredCacheManagerEnforceJmxDomain(String cacheManagerName, String jmxDomain, boolean allowDuplicateDomains) {
      return createClusteredCacheManagerEnforceJmxDomain(cacheManagerName, jmxDomain, true, allowDuplicateDomains, new ConfigurationBuilder(), new PerThreadMBeanServerLookup());
   }

   public static EmbeddedCacheManager createClusteredCacheManagerEnforceJmxDomain(String jmxDomain, ConfigurationBuilder builder) {
      return createClusteredCacheManagerEnforceJmxDomain(jmxDomain, true, false, builder);
   }

   public static EmbeddedCacheManager createClusteredCacheManagerEnforceJmxDomain(
         String jmxDomain, boolean exposeGlobalJmx, boolean allowDuplicateDomains, ConfigurationBuilder builder) {
      return createClusteredCacheManagerEnforceJmxDomain(null, jmxDomain,
            exposeGlobalJmx, allowDuplicateDomains, builder, new PerThreadMBeanServerLookup());
   }

   public static EmbeddedCacheManager createClusteredCacheManagerEnforceJmxDomain(
         String cacheManagerName, String jmxDomain, boolean exposeGlobalJmx, boolean allowDuplicateDomains, ConfigurationBuilder builder,
         MBeanServerLookup mBeanServerLookup) {
      GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      amendGlobalConfiguration(globalBuilder, new TransportFlags());
      globalBuilder.globalJmxStatistics()
            .jmxDomain(jmxDomain)
            .mBeanServerLookup(mBeanServerLookup)
            .allowDuplicateDomains(allowDuplicateDomains)
            .enabled(exposeGlobalJmx);
      if (cacheManagerName != null) {
         globalBuilder.globalJmxStatistics().cacheManagerName(cacheManagerName);
      }
      return createClusteredCacheManager(globalBuilder, builder, new TransportFlags(), true);
   }

   /**
    * @see #createCacheManagerEnforceJmxDomain(String)
    */
   public static EmbeddedCacheManager createCacheManagerEnforceJmxDomain(String jmxDomain, boolean exposeGlobalJmx, boolean exposeCacheJmx) {
      return createCacheManagerEnforceJmxDomain(jmxDomain, null, exposeGlobalJmx, exposeCacheJmx);
   }

   /**
    * @see #createCacheManagerEnforceJmxDomain(String)
    */
   public static EmbeddedCacheManager createCacheManagerEnforceJmxDomain(String jmxDomain, String cacheManagerName, boolean exposeGlobalJmx, boolean exposeCacheJmx) {
      GlobalConfigurationBuilder globalConfiguration = new GlobalConfigurationBuilder();
      globalConfiguration
         .globalJmxStatistics()
            .jmxDomain(jmxDomain)
            .mBeanServerLookup(new PerThreadMBeanServerLookup())
            .enabled(exposeGlobalJmx);
      if (cacheManagerName != null)
         globalConfiguration.globalJmxStatistics().cacheManagerName(cacheManagerName);
      ConfigurationBuilder configuration = new ConfigurationBuilder();
      configuration.jmxStatistics().enabled(exposeCacheJmx);
      return createCacheManager(globalConfiguration, configuration, true);
   }

   public static ConfigurationBuilder getDefaultCacheConfiguration(boolean transactional) {
      return getDefaultCacheConfiguration(transactional, false);
   }

   public static ConfigurationBuilder getDefaultCacheConfiguration(boolean transactional, boolean useCustomTxLookup) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      markAsTransactional(transactional, builder);
      //don't changed the tx lookup.
      if (useCustomTxLookup) updateTransactionSupport(transactional, builder);
      return builder;
   }

   public static void amendTransport(GlobalConfigurationBuilder cfg) {
      amendTransport(cfg, new TransportFlags());
   }

   private static void amendTransport(GlobalConfigurationBuilder builder, TransportFlags flags) {
      org.infinispan.configuration.global.GlobalConfiguration gc = builder.build();
      if (gc.transport().transport() != null) { //this is local
         String testName = TestResourceTracker.getCurrentTestName();
         String nextCacheName = TestResourceTracker.getNextNodeName();

         // Remove any configuration file that might have been set.
         builder.transport().removeProperty(JGroupsTransport.CONFIGURATION_FILE);

         builder
               .transport()
               .addProperty(JGroupsTransport.CONFIGURATION_STRING, getJGroupsConfig(testName, flags))
               .nodeName(nextCacheName);
      }
   }

   public static void minimizeThreads(GlobalConfigurationBuilder builder) {
      BlockingThreadPoolExecutorFactory executorFactory = new BlockingThreadPoolExecutorFactory(
            MAX_ASYNC_EXEC_THREADS, MAX_ASYNC_EXEC_THREADS, ASYNC_EXEC_QUEUE_SIZE, KEEP_ALIVE);
      builder.transport().transportThreadPool().threadPoolFactory(executorFactory);

      executorFactory = new BlockingThreadPoolExecutorFactory(
            MAX_REQ_EXEC_THREADS, MAX_REQ_EXEC_THREADS, REQ_EXEC_QUEUE_SIZE, KEEP_ALIVE);
      builder.transport().remoteCommandThreadPool().threadPoolFactory(executorFactory);
   }

   public static void amendMarshaller(GlobalConfigurationBuilder builder) {
      if (MARSHALLER != null) {
         try {
            Marshaller marshaller = Util.getInstanceStrict(MARSHALLER, Thread.currentThread().getContextClassLoader());
            builder.serialization().marshaller(marshaller);
         } catch (ClassNotFoundException e) {
            // No-op, stick to GlobalConfiguration default.
         } catch (InstantiationException e) {
            // No-op, stick to GlobalConfiguration default.
         } catch (IllegalAccessException e) {
            // No-op, stick to GlobalConfiguration default.
         }
      }
   }

   private static DefaultCacheManager newDefaultCacheManager(boolean start, GlobalConfigurationBuilder gc, ConfigurationBuilder c) {
      GlobalConfiguration globalConfiguration = gc.build();
      DefaultCacheManager defaultCacheManager = new DefaultCacheManager(globalConfiguration, c.build(globalConfiguration), start);
      TestResourceTracker.addResource(new TestResourceTracker.CacheManagerCleaner(defaultCacheManager));
      return defaultCacheManager;
   }

   private static DefaultCacheManager newDefaultCacheManager(boolean start, ConfigurationBuilderHolder holder) {
      DefaultCacheManager defaultCacheManager = new DefaultCacheManager(holder, start);
      TestResourceTracker.addResource(new TestResourceTracker.CacheManagerCleaner(defaultCacheManager));
      return defaultCacheManager;
   }

   public static ConfigurationBuilderHolder buildAggregateHolder(String... xmls)
         throws XMLStreamException, FactoryConfigurationError {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      ParserRegistry registry = new ParserRegistry(cl);

      ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder(cl);
      for (String xml : xmls) {
         registry.parse(new ByteArrayInputStream(xml.getBytes()), holder);
      }

      return holder;
   }
}
