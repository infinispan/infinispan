package org.infinispan.test.fwk;

import static org.infinispan.test.fwk.JGroupsConfigBuilder.getJGroupsConfig;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.infinispan.commands.module.TestGlobalConfigurationBuilder;
import org.infinispan.commons.configuration.io.ConfigurationResourceResolvers;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.executors.ThreadPoolExecutorFactory;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.PlatformMBeanServerLookup;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.InstanceSupplier;
import org.infinispan.commons.util.LegacyKeySupportSystemProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.ThreadPoolConfiguration;
import org.infinispan.configuration.global.TransportConfiguration;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.threads.CoreExecutorFactory;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.CallInterceptor;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.security.Security;
import org.infinispan.transaction.TransactionMode;
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
   public static final String DEFAULT_CACHE_NAME = "defaultcache";
   public static final int NAMED_EXECUTORS_THREADS_NO_QUEUE = 6;
   // Check *TxPartitionAndMerge*Test with taskset -c 1 before reducing the following 2
   private static final int NAMED_EXECUTORS_THREADS_WITH_QUEUE = 6;
   private static final int NAMED_EXECUTORS_QUEUE_SIZE = 20;
   private static final int NAMED_EXECUTORS_KEEP_ALIVE = 30000;

   private static final String MARSHALLER = LegacyKeySupportSystemProperties.getProperty(
         "infinispan.test.marshaller.class", "infinispan.marshaller.class");

   private static final Log log = LogFactory.getLog(TestCacheManagerFactory.class);

   /**
    * Note this method does not amend the global configuration to reduce overall resource footprint.  It is therefore
    * suggested to use
    * {@link
    * org.infinispan.test.fwk.TestCacheManagerFactory#createClusteredCacheManager(org.infinispan.configuration.cache.ConfigurationBuilder,
    * TransportFlags)} instead when this is needed
    *
    * @param start Whether to start this cache container
    * @param gcb   The global configuration builder to use
    * @param c     The default configuration to use
    * @return The resultant cache manager that is created
    */
   public static EmbeddedCacheManager newDefaultCacheManager(boolean start, GlobalConfigurationBuilder gcb,
                                                             ConfigurationBuilder c) {
      setNodeName(gcb);
      GlobalConfiguration globalConfiguration = gcb.build();
      checkJmx(globalConfiguration);
      DefaultCacheManager defaultCacheManager;
      if (c != null) {
         amendDefaultCache(gcb);
         ConfigurationBuilderHolder holder =
               new ConfigurationBuilderHolder(Thread.currentThread().getContextClassLoader(), gcb);
         holder.getNamedConfigurationBuilders().put(gcb.defaultCacheName().get(), c);
         defaultCacheManager = new DefaultCacheManager(holder, start);
      } else {
         defaultCacheManager = new DefaultCacheManager(gcb.build(), start);
      }
      TestResourceTracker.addResource(new CacheManagerCleaner(defaultCacheManager));
      return defaultCacheManager;
   }

   public static DefaultCacheManager newDefaultCacheManager(boolean start, ConfigurationBuilderHolder holder) {
      GlobalConfigurationBuilder gcb = holder.getGlobalConfigurationBuilder();
      if (holder.getDefaultConfigurationBuilder() != null) {
         amendDefaultCache(gcb);
      }
      setNodeName(gcb);
      checkJmx(gcb.build());
      DefaultCacheManager defaultCacheManager = new DefaultCacheManager(holder, start);
      TestResourceTracker.addResource(new CacheManagerCleaner(defaultCacheManager));
      return defaultCacheManager;
   }

   public static EmbeddedCacheManager fromXml(String xmlFile) throws IOException {
      return fromXml(xmlFile, false);
   }

   public static EmbeddedCacheManager fromXml(String xmlFile, boolean defaultParserOnly) throws IOException {
      return fromXml(xmlFile, defaultParserOnly, true);
   }

   public static EmbeddedCacheManager fromXml(String xmlFile, boolean defaultParserOnly, boolean start) throws IOException {
      return fromXml(xmlFile, defaultParserOnly, start, new TransportFlags());
   }

   public static EmbeddedCacheManager fromXml(String xmlFile, boolean defaultParserOnly, boolean start, TransportFlags transportFlags) throws IOException {
      // Use parseURL because it sets an XMLResourceResolver and allows includes
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      URL url = FileLookupFactory.newInstance().lookupFileLocation(xmlFile, classLoader);
      ConfigurationBuilderHolder holder = parseURL(url, defaultParserOnly);
      return createClusteredCacheManager(start, holder, transportFlags);
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
      ConfigurationBuilderHolder holder = parserRegistry.parse(is, ConfigurationResourceResolvers.DEFAULT, MediaType.APPLICATION_XML);
      return updateTestName(holder);
   }

   private static ConfigurationBuilderHolder updateTestName(ConfigurationBuilderHolder holder) {
      // The node name is set in each DefaultThreadFactory individually, override it here
      String testShortName = TestResourceTracker.getCurrentTestShortName();
      GlobalConfiguration gc = holder.getGlobalConfigurationBuilder().build();
      updateNodeName(testShortName, gc.listenerThreadPool());
      updateNodeName(testShortName, gc.expirationThreadPool());
      updateNodeName(testShortName, gc.persistenceThreadPool());
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
         builder.transaction().transactionManagerLookup(Util.getInstance(TransactionSetup.getManagerLookup(), TestCacheManagerFactory.class.getClassLoader()));
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
      return createClusteredCacheManager(true, gcb, defaultCacheConfig, flags);
   }

   public static EmbeddedCacheManager createClusteredCacheManager(boolean start, GlobalConfigurationBuilder gcb,
                                                                  ConfigurationBuilder defaultCacheConfig,
                                                                  TransportFlags flags) {
      amendGlobalConfiguration(gcb, flags);
      if (defaultCacheConfig != null) {
         amendJTA(defaultCacheConfig);
      }
      return newDefaultCacheManager(start, gcb, defaultCacheConfig);
   }

   public static EmbeddedCacheManager createClusteredCacheManager(boolean start, SerializationContextInitializer sci,
                                                                  ConfigurationBuilder defaultCacheConfig,
                                                                  TransportFlags flags) {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      if (sci != null) gcb.serialization().addContextInitializer(sci);
      amendGlobalConfiguration(gcb, flags);
      if (defaultCacheConfig != null) {
         amendJTA(defaultCacheConfig);
      }
      return newDefaultCacheManager(start, gcb, defaultCacheConfig);
   }

   public static void amendGlobalConfiguration(GlobalConfigurationBuilder gcb, TransportFlags flags) {
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
      return createCacheManager((ConfigurationBuilder) null);
   }

   public static EmbeddedCacheManager createServerModeCacheManager() {
      return createServerModeCacheManager((ConfigurationBuilder) null);
   }

   public static EmbeddedCacheManager createServerModeCacheManager(ConfigurationBuilder builder) {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalBuilder.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      return createCacheManager(globalBuilder, builder);
   }

   public static EmbeddedCacheManager createServerModeCacheManager(SerializationContextInitializer sci) {
      return createServerModeCacheManager(sci, null);
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
      return newDefaultCacheManager(start, globalBuilder, null);
   }

   public static EmbeddedCacheManager createCacheManager(GlobalConfigurationBuilder globalBuilder,
                                                         ConfigurationBuilder builder) {
      return createCacheManager(globalBuilder, builder, true);
   }

   public static EmbeddedCacheManager createCacheManager(GlobalConfigurationBuilder globalBuilder,
                                                         ConfigurationBuilder builder, boolean start) {
      if (globalBuilder.transport().build().transport().transport() != null) {
         throw new IllegalArgumentException(
               "Use TestCacheManagerFactory.createClusteredCacheManager(...) for clustered cache managers");
      }
      amendTransport(globalBuilder);
      return newDefaultCacheManager(start, globalBuilder, builder);
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
      builder.clustering().cacheMode(mode);
      if (indexing) {
         builder.indexing().enabled(true);
      }
      if (mode.isClustered()) {
         return createClusteredCacheManager(globalBuilder, builder);
      } else {
         return createCacheManager(globalBuilder, builder);
      }
   }

   public static void configureJmx(GlobalConfigurationBuilder builder, String jmxDomain,
                                   MBeanServerLookup mBeanServerLookup) {
      builder.jmx().enabled(true)
            .domain(jmxDomain)
            .mBeanServerLookup(mBeanServerLookup);
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
      if (builder.build().transport().nodeName() != null)
         return;

      String nextNodeName = TestResourceTracker.getNextNodeName();

      // Set the node name even for local managers in order to set the name of the worker threads
      builder.transport().nodeName(nextNodeName);
   }

   public static void minimizeThreads(GlobalConfigurationBuilder builder) {
      ThreadPoolExecutorFactory executorFactoryWithQueue =
            CoreExecutorFactory.executorFactory(NAMED_EXECUTORS_THREADS_WITH_QUEUE, NAMED_EXECUTORS_THREADS_WITH_QUEUE,
                  NAMED_EXECUTORS_QUEUE_SIZE, NAMED_EXECUTORS_KEEP_ALIVE, false);
      ThreadPoolExecutorFactory nonBlockingExecutorFactoryWithQueue =
            CoreExecutorFactory.executorFactory(NAMED_EXECUTORS_THREADS_WITH_QUEUE, NAMED_EXECUTORS_THREADS_WITH_QUEUE,
                  NAMED_EXECUTORS_QUEUE_SIZE, NAMED_EXECUTORS_KEEP_ALIVE, true);

      builder.blockingThreadPool().threadPoolFactory(executorFactoryWithQueue);
      builder.nonBlockingThreadPool().threadPoolFactory(nonBlockingExecutorFactoryWithQueue);
      // Listener thread pool already has a single thread
      // builder.listenerThreadPool().threadPoolFactory(executorFactoryWithQueue);
      // TODO Scheduled thread pools don't have a threads limit
      // Timeout thread pool is not configurable at all
   }

   public static void amendDefaultCache(GlobalConfigurationBuilder builder) {
      if (!builder.defaultCacheName().isPresent()) {
         builder.defaultCacheName(DEFAULT_CACHE_NAME);
      }
   }

   public static void amendMarshaller(GlobalConfigurationBuilder builder) {
      if (MARSHALLER != null) {
         try {
            Marshaller marshaller = Util.getInstanceStrict(MARSHALLER, Thread.currentThread().getContextClassLoader());
            builder.serialization().marshaller(marshaller);
         } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException |
                  InvocationTargetException e) {
            // No-op, stick to GlobalConfiguration default.
         }
      }
   }

   private static void checkJmx(GlobalConfiguration gc) {
      assert !(gc.jmx().enabled() && gc.jmx().mbeanServerLookup() instanceof PlatformMBeanServerLookup)
            : "Tests must configure a MBeanServerLookup other than the default PlatformMBeanServerLookup or not enable JMX";
   }

   public static GlobalConfigurationBuilder addInterceptor(GlobalConfigurationBuilder global, Predicate<String> namePredicate, AsyncInterceptor interceptor,
                                                           InterceptorPosition position, Class<? extends AsyncInterceptor> otherClass) {
      return addInterceptor(global, namePredicate, new InstanceSupplier<>(interceptor), position, otherClass);
   }

   public static GlobalConfigurationBuilder addInterceptor(GlobalConfigurationBuilder global, Predicate<String> namePredicate, Supplier<AsyncInterceptor> interceptorSupplier,
                                                           InterceptorPosition position, Class<? extends AsyncInterceptor> otherClass) {
      global.addModule(TestGlobalConfigurationBuilder.class).addCacheStartingCallback(cr -> {
         if (namePredicate.test(cr.getCacheName())) {
            AsyncInterceptor interceptor = interceptorSupplier.get();
            BasicComponentRegistry bcr = cr.getComponent(BasicComponentRegistry.class);
            bcr.registerComponent(interceptor.getClass(), interceptor, true);
            bcr.addDynamicDependency(AsyncInterceptorChain.class.getName(), interceptor.getClass().getName());
            AsyncInterceptorChain chain = bcr.getComponent(AsyncInterceptorChain.class).wired();
            switch (position) {
               case FIRST:
                  chain.addInterceptor(interceptor, 0);
                  break;
               case BEFORE:
                  chain.addInterceptorBefore(interceptor, otherClass);
                  break;
               case REPLACE:
                  chain.replaceInterceptor(interceptor, otherClass);
                  break;
               case AFTER:
                  chain.addInterceptorAfter(interceptor, otherClass);
                  break;
               case LAST:
                  chain.addInterceptorBefore(interceptor, CallInterceptor.class);
                  break;
            }
         }
      });
      return global;
   }

   public enum InterceptorPosition {
      FIRST,
      BEFORE,
      REPLACE,
      AFTER,
      LAST
   }

   public static class CacheManagerCleaner extends TestResourceTracker.Cleaner<EmbeddedCacheManager> {

      protected CacheManagerCleaner(EmbeddedCacheManager ref) {
         super(ref);
      }

      @Override
      public void close() {
         Runnable action = () -> {
            if (!ref.getStatus().isTerminated()) {
               TestCacheManagerFactory.log.debugf("Stopping cache manager %s", ref);
               ref.stop();
            }
         };
         Security.doPrivileged(action);
      }
   }
}
