package org.infinispan.test.fwk;

import static org.infinispan.test.fwk.JGroupsConfigBuilder.getJGroupsConfig;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.PlatformMBeanServerLookup;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.LegacyKeySupportSystemProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.ThreadPoolConfiguration;
import org.infinispan.configuration.global.TransportConfiguration;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.transaction.TransactionMode;

/**
 * CacheManagers in unit tests should be created with this factory, in order to avoid resource clashes. See
 * http://community.jboss.org/wiki/ParallelTestSuite for more details.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
public class TestCacheManagerFactory {
   public static final int NAMED_EXECUTORS_THREADS_NO_QUEUE = 6;
   // Check *TxPartitionAndMerge*Test with taskset -c 1 before reducing the following 2
   private static final int NAMED_EXECUTORS_THREADS_WITH_QUEUE = 6;
   private static final int NAMED_EXECUTORS_QUEUE_SIZE = 20;
   private static final int NAMED_EXECUTORS_KEEP_ALIVE = 30000;

   private static final String MARSHALLER = LegacyKeySupportSystemProperties.getProperty(
      "infinispan.test.marshaller.class", "infinispan.marshaller.class");

   /**
    * Note this method does not amend the global configuration to reduce overall resource footprint.  It is therefore
    * suggested to use {@link org.infinispan.test.fwk.TestCacheManagerFactory#createClusteredCacheManager(org.infinispan.configuration.cache.ConfigurationBuilder,
    * TransportFlags)} instead when this is needed
    *
    * @param start         Whether to start this cache container
    * @param gcb           The global configuration builder to use
    * @param c             The default configuration to use
    * @return The resultant cache manager that is created
    */
   public static EmbeddedCacheManager newDefaultCacheManager(boolean start, GlobalConfigurationBuilder gcb,
                                                             ConfigurationBuilder c) {
      if (c != null) {
         amendDefaultCache(gcb);
      }
      setNodeName(gcb);
      TestApplicationMetricsRegistry.replace(gcb);
      GlobalConfiguration globalConfiguration = gcb.build();
      checkJmx(globalConfiguration);
      DefaultCacheManager defaultCacheManager = new DefaultCacheManager(globalConfiguration, c == null ? null : c.build(globalConfiguration), start);
      TestResourceTracker.addResource(new TestResourceTracker.CacheManagerCleaner(defaultCacheManager));
      return defaultCacheManager;
   }

   private static DefaultCacheManager newDefaultCacheManager(boolean start, ConfigurationBuilderHolder holder) {
      GlobalConfigurationBuilder gcb = holder.getGlobalConfigurationBuilder();
      amendDefaultCache(gcb);
      setNodeName(gcb);
      TestApplicationMetricsRegistry.replace(gcb);
      checkJmx(gcb.build());
      DefaultCacheManager defaultCacheManager = new DefaultCacheManager(holder, start);
      TestResourceTracker.addResource(new TestResourceTracker.CacheManagerCleaner(defaultCacheManager));
      return defaultCacheManager;
   }

   public static EmbeddedCacheManager fromXml(String xmlFile) throws IOException {
      return fromXml(xmlFile, false);
   }

   public static EmbeddedCacheManager fromXml(String xmlFile, boolean defaultParserOnly) throws IOException {
      return fromXml(xmlFile, defaultParserOnly, true);
   }

   public static EmbeddedCacheManager fromXml(String xmlFile, boolean defaultParserOnly, boolean start) throws IOException {
      // Use parseURL because it sets an XMLResourceResolver and allows includes
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      URL url = FileLookupFactory.newInstance().lookupFileLocation(xmlFile, classLoader);
      ConfigurationBuilderHolder holder = parseURL(url, defaultParserOnly);
      return createClusteredCacheManager(start, holder, new TransportFlags());
   }

   public static EmbeddedCacheManager fromStream(InputStream is) {
      return fromStream(is, true);
   }

   public static EmbeddedCacheManager fromStream(InputStream is, boolean defaultParsersOnly) {
      return fromStream(is, defaultParsersOnly, true);
   }

   public static EmbeddedCacheManager fromStream(InputStream is, boolean defaultParsersOnly, boolean start) {
      return createClusteredCacheManager(start, parseStream(is, defaultParsersOnly), new TransportFlags());
   }

   public static ConfigurationBuilderHolder parseFile(String xmlFile, boolean defaultParserOnly) throws IOException {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      try (InputStream is = FileLookupFactory.newInstance().lookupFileStrict(xmlFile, classLoader)) {
         return parseStream(is, defaultParserOnly);
      }
   }

   public static ConfigurationBuilderHolder parseURL(URL url, boolean defaultParsersOnly) throws IOException {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      ParserRegistry parserRegistry = new ParserRegistry(classLoader, defaultParsersOnly, System.getProperties());
      ConfigurationBuilderHolder holder = parserRegistry.parse(url);
      return updateTestName(holder);
   }

   public static ConfigurationBuilderHolder parseStream(InputStream is, boolean defaultParsersOnly) {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      ParserRegistry parserRegistry = new ParserRegistry(classLoader, defaultParsersOnly, System.getProperties());
      ConfigurationBuilderHolder holder = parserRegistry.parse(is, null);
      return updateTestName(holder);
   }

   private static ConfigurationBuilderHolder updateTestName(ConfigurationBuilderHolder holder) {
      // The node name is set in each DefaultThreadFactory individually, override it here
      String testShortName = TestResourceTracker.getCurrentTestShortName();
      GlobalConfiguration gc = holder.getGlobalConfigurationBuilder().build();
      updateNodeName(testShortName, gc.listenerThreadPool());
      updateNodeName(testShortName, gc.expirationThreadPool());
      updateNodeName(testShortName, gc.persistenceThreadPool());
      updateNodeName(testShortName, gc.stateTransferThreadPool());
      updateNodeName(testShortName, gc.asyncThreadPool());
      updateNodeName(testShortName, gc.transport().transportThreadPool());
      return holder;
   }

   private static void updateNodeName(String nodeName, ThreadPoolConfiguration threadPoolConfiguration) {
      if (threadPoolConfiguration.threadFactory() instanceof DefaultThreadFactory) {
         ((DefaultThreadFactory) threadPoolConfiguration.threadFactory()).setNode(nodeName);
      }
   }

   public static EmbeddedCacheManager fromString(String config) {
      return fromStream(new ByteArrayInputStream(config.getBytes()));
   }

   private static void markAsTransactional(boolean transactional, ConfigurationBuilder builder) {
      if (!transactional) {
         builder.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      } else {
         builder.transaction()
               .transactionMode(TransactionMode.TRANSACTIONAL);
         if (!Util.isOSGiContext()) {
            //automatically change default TM lookup to the desired one but only outside OSGi. In OSGi we need to use GenericTransactionManagerLookup
            builder.transaction().transactionManagerLookup(Util.getInstance(TransactionSetup.getManagerLookup(), TestCacheManagerFactory.class.getClassLoader()));
         }
      }
   }

   private static void updateTransactionSupport(boolean transactional, ConfigurationBuilder builder) {
      if (transactional) amendJTA(builder);
   }

   private static void amendJTA(ConfigurationBuilder builder) {
      if (builder.transaction().transactionMode() == TransactionMode.TRANSACTIONAL && builder.transaction().transactionManagerLookup() == null) {
         builder.transaction().transactionManagerLookup(Util.getInstance(TransactionSetup.getManagerLookup(), TestCacheManagerFactory.class.getClassLoader()));
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

   public static EmbeddedCacheManager createClusteredCacheManager(SerializationContextInitializer sci) {
      return createClusteredCacheManager(sci, new ConfigurationBuilder());
   }

   public static EmbeddedCacheManager createClusteredCacheManager(SerializationContextInitializer sci, ConfigurationBuilder defaultCacheConfig) {
      GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      if (sci != null) globalBuilder.serialization().addContextInitializer(sci);
      return createClusteredCacheManager(globalBuilder, defaultCacheConfig, new TransportFlags());
   }

   public static EmbeddedCacheManager createClusteredCacheManager(GlobalConfigurationBuilder gcb, ConfigurationBuilder defaultCacheConfig) {
      return createClusteredCacheManager(gcb, defaultCacheConfig, new TransportFlags());
   }

   public static EmbeddedCacheManager createClusteredCacheManager(ConfigurationBuilder defaultCacheConfig) {
      return createClusteredCacheManager(GlobalConfigurationBuilder.defaultClusteredBuilder(), defaultCacheConfig);
   }

   public static EmbeddedCacheManager createClusteredCacheManager(ConfigurationBuilderHolder holder) {
      return createClusteredCacheManager(true, holder);
   }

   public static EmbeddedCacheManager createClusteredCacheManager(boolean start, ConfigurationBuilderHolder holder) {
      return createClusteredCacheManager(start, holder, new TransportFlags());
   }

   public static EmbeddedCacheManager createClusteredCacheManager(boolean start, ConfigurationBuilderHolder holder,
                                                                  TransportFlags flags) {
      amendGlobalConfiguration(holder.getGlobalConfigurationBuilder(), flags);
      for (ConfigurationBuilder builder : holder.getNamedConfigurationBuilders().values())
         amendJTA(builder);

      return newDefaultCacheManager(start, holder);
   }

   public static EmbeddedCacheManager createClusteredCacheManager(GlobalConfigurationBuilder gcb,
                                                                  ConfigurationBuilder defaultCacheConfig,
                                                                  TransportFlags flags) {
      amendGlobalConfiguration(gcb, flags);
      amendJTA(defaultCacheConfig);
      return newDefaultCacheManager(true, gcb, defaultCacheConfig);
   }

   public static EmbeddedCacheManager createClusteredCacheManager(boolean start, GlobalConfigurationBuilder gcb,
                                                                  ConfigurationBuilder defaultCacheConfig,
                                                                  TransportFlags flags) {
      amendGlobalConfiguration(gcb, flags);
      amendJTA(defaultCacheConfig);
      return newDefaultCacheManager(start, gcb, defaultCacheConfig);
   }

   public static void amendGlobalConfiguration(GlobalConfigurationBuilder gcb, TransportFlags flags) {
      amendDefaultCache(gcb);
      amendMarshaller(gcb);
      minimizeThreads(gcb);
      amendTransport(gcb, flags);
   }

   public static EmbeddedCacheManager createCacheManager(ConfigurationBuilder builder) {
      return createCacheManager(new GlobalConfigurationBuilder().nonClusteredDefault(), builder);
   }

   public static EmbeddedCacheManager createCacheManager(SerializationContextInitializer sci,
                                                         ConfigurationBuilder builder) {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      if (sci != null)
         globalBuilder.serialization().addContextInitializer(sci);
      return createCacheManager(globalBuilder, builder);
   }

   public static EmbeddedCacheManager createCacheManager() {
      return createCacheManager(new ConfigurationBuilder());
   }

   public static EmbeddedCacheManager createServerModeCacheManager() {
      return createServerModeCacheManager(new ConfigurationBuilder());
   }

   public static EmbeddedCacheManager createServerModeCacheManager(ConfigurationBuilder builder) {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalBuilder.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      return createCacheManager(globalBuilder, builder);
   }

   public static EmbeddedCacheManager createServerModeCacheManager(SerializationContextInitializer sci, ConfigurationBuilder builder) {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalBuilder.serialization().addContextInitializer(sci);
      return createServerModeCacheManager(globalBuilder, builder);
   }


   public static EmbeddedCacheManager createServerModeCacheManager(GlobalConfigurationBuilder gcb) {
      gcb.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      return createCacheManager(gcb, new ConfigurationBuilder());
   }

   public static EmbeddedCacheManager createServerModeCacheManager(GlobalConfigurationBuilder gcb, ConfigurationBuilder builder) {
      gcb.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      return createCacheManager(gcb, builder);
   }

   public static EmbeddedCacheManager createCacheManager(boolean start) {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      amendGlobalConfiguration(globalBuilder, new TransportFlags());
      return newDefaultCacheManager(start, globalBuilder, new ConfigurationBuilder());
   }

   public static EmbeddedCacheManager createCacheManager(SerializationContextInitializer sci) {
      return createCacheManager(true, sci);
   }

   public static EmbeddedCacheManager createCacheManager(boolean start, SerializationContextInitializer sci) {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      amendGlobalConfiguration(globalBuilder, new TransportFlags());
      if (sci != null) globalBuilder.serialization().addContextInitializer(sci);
      return newDefaultCacheManager(start, globalBuilder, new ConfigurationBuilder());
   }

   public static EmbeddedCacheManager createCacheManager(GlobalConfigurationBuilder globalBuilder, ConfigurationBuilder builder) {
      if (globalBuilder.transport().build().transport().transport() != null) {
         throw new IllegalArgumentException("Use TestCacheManagerFactory.createClusteredCacheManager(...) for clustered cache managers");
      }
      amendTransport(globalBuilder);
      return newDefaultCacheManager(true, globalBuilder, builder);
   }

   public static EmbeddedCacheManager createCacheManager(CacheMode mode, boolean indexing) {
      return createCacheManager(null, mode, indexing);
   }

   public static EmbeddedCacheManager createCacheManager(SerializationContextInitializer sci, CacheMode mode, boolean indexing) {
      GlobalConfigurationBuilder globalBuilder = mode.isClustered() ?
            new GlobalConfigurationBuilder().clusteredDefault() :
            new GlobalConfigurationBuilder().nonClusteredDefault();

      if (sci != null)
         globalBuilder.serialization().addContextInitializer(sci);

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(mode)
             .indexing().index(indexing ? Index.ALL : Index.NONE)
             .addProperty("lucene_version", "LUCENE_CURRENT");
      if (mode.isClustered()) {
         return createClusteredCacheManager(globalBuilder, builder);
      } else {
         return createCacheManager(globalBuilder, builder);
      }
   }

   public static void configureGlobalJmx(GlobalConfigurationBuilder builder, String jmxDomain,
                                         MBeanServerLookup mBeanServerLookup) {
      builder.cacheContainer().statistics(true);
      builder.globalJmxStatistics()
             .jmxDomain(jmxDomain)
             .mBeanServerLookup(mBeanServerLookup);
      // In case we change the default back
      builder.globalJmxStatistics().allowDuplicateDomains(false);
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
      String testName = TestResourceTracker.getCurrentTestName();

      GlobalConfiguration gc = builder.build();
      if (!flags.isPreserveConfig() && gc.transport().transport() != null) {
         if (flags.isRelayRequired()) {
            // Respect siteName transport flag
            builder.transport().clusterName(flags.siteName() + "-" + testName);
         } else if (gc.transport().attributes().attribute(TransportConfiguration.CLUSTER_NAME).isModified()) {
            // Respect custom cluster name (e.g. from TestCluster)
            builder.transport().clusterName(gc.transport().clusterName() + "-" + testName);
         } else {
            builder.transport().clusterName(testName);
         }
         // Remove any configuration file that might have been set.
         builder.transport().removeProperty(JGroupsTransport.CONFIGURATION_FILE);
         builder.transport().removeProperty(JGroupsTransport.CHANNEL_CONFIGURATOR);

         builder.transport().addProperty(JGroupsTransport.CONFIGURATION_STRING, getJGroupsConfig(testName, flags));
      }
   }

   public static void setNodeName(GlobalConfigurationBuilder builder) {
      String nextNodeName = TestResourceTracker.getNextNodeName();

      // Set the node name even for local managers in order to set the name of the worker threads
      builder.transport().nodeName(nextNodeName);
   }

   public static void minimizeThreads(GlobalConfigurationBuilder builder) {
      BlockingThreadPoolExecutorFactory executorFactoryNoQueue =
         new BlockingThreadPoolExecutorFactory(NAMED_EXECUTORS_THREADS_NO_QUEUE, 0, 0, NAMED_EXECUTORS_KEEP_ALIVE);
      BlockingThreadPoolExecutorFactory executorFactoryWithQueue =
         new BlockingThreadPoolExecutorFactory(NAMED_EXECUTORS_THREADS_WITH_QUEUE, NAMED_EXECUTORS_THREADS_WITH_QUEUE,
                                               NAMED_EXECUTORS_QUEUE_SIZE, NAMED_EXECUTORS_KEEP_ALIVE);
      BlockingThreadPoolExecutorFactory nonBlockingExecutorFactoryWithQueue =
            new BlockingThreadPoolExecutorFactory(NAMED_EXECUTORS_THREADS_WITH_QUEUE, NAMED_EXECUTORS_THREADS_WITH_QUEUE,
                  NAMED_EXECUTORS_QUEUE_SIZE, NAMED_EXECUTORS_KEEP_ALIVE, true);

      builder.transport().remoteCommandThreadPool().threadPoolFactory(executorFactoryNoQueue);
      builder.stateTransferThreadPool().threadPoolFactory(executorFactoryNoQueue);

      builder.persistenceThreadPool().threadPoolFactory(executorFactoryWithQueue);
      builder.asyncThreadPool().threadPoolFactory(nonBlockingExecutorFactoryWithQueue);
      builder.listenerThreadPool().threadPoolFactory(executorFactoryWithQueue);
      builder.transport().transportThreadPool().threadPoolFactory(executorFactoryWithQueue);
      // TODO Scheduled thread pools don't have a threads limit
      // Timeout thread pool is not configurable at all
   }

   public static void amendDefaultCache(GlobalConfigurationBuilder builder) {
      if (!builder.defaultCacheName().isPresent()) {
         builder.defaultCacheName(TestResourceTracker.getCurrentTestShortName());
      }
   }

   public static void amendMarshaller(GlobalConfigurationBuilder builder) {
      if (MARSHALLER != null) {
         try {
            Marshaller marshaller = Util.getInstanceStrict(MARSHALLER, Thread.currentThread().getContextClassLoader());
            builder.serialization().marshaller(marshaller);
         } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            // No-op, stick to GlobalConfiguration default.
         }
      }
   }

   private static void checkJmx(GlobalConfiguration gc) {
      // Statistics are now disabled by default (since ISPN-10723)
      // But in case they get enabled by default again, make sure they don't get enabled
      // with the PlatformMBeanServerLookup
      assert !(gc.globalJmxStatistics().mbeanServerLookup() instanceof PlatformMBeanServerLookup);
   }
}
