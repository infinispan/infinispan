package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.impl.Util.await;
import static org.infinispan.client.hotrod.impl.Util.checkTransactionSupport;
import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.transaction.xa.XAResource;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.configuration.RemoteCacheConfiguration;
import org.infinispan.client.hotrod.configuration.StatisticsConfiguration;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.counter.impl.RemoteCounterManager;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.HotRodURI;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.impl.MarshallerRegistry;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.RemoteCacheManagerAdminImpl;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.operations.PingResponse;
import org.infinispan.client.hotrod.impl.protocol.CodecHolder;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transaction.SyncModeTransactionTable;
import org.infinispan.client.hotrod.impl.transaction.TransactionOperationFactory;
import org.infinispan.client.hotrod.impl.transaction.TransactionTable;
import org.infinispan.client.hotrod.impl.transaction.TransactionalRemoteCacheImpl;
import org.infinispan.client.hotrod.impl.transaction.XaModeTransactionTable;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.jmx.RemoteCacheManagerMXBean;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.marshall.BytesOnlyMarshaller;
import org.infinispan.client.hotrod.near.NearCacheService;
import org.infinispan.client.hotrod.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.commons.marshall.UserContextInitializerImpl;
import org.infinispan.commons.time.DefaultTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.GlobUtils;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;

import jakarta.transaction.TransactionManager;

/**
 * <p>Factory for {@link RemoteCache}s.</p>
 * <p>In order to be able to use a {@link RemoteCache}, the
 * {@link RemoteCacheManager} must be started first: this instantiates connections to Hot Rod server(s). Starting the
 * {@link RemoteCacheManager} can be done either at creation by passing start==true to the constructor or by using a
 * constructor that does that for you; or after construction by calling {@link #start()}.</p>
 * <p><b>NOTE:</b> this is an "expensive" object, as it manages a set of persistent TCP connections to the Hot Rod
 * servers. It is recommended to only have one instance of this per JVM, and to cache it between calls to the server
 * (i.e. remoteCache operations)</p>
 * <p>{@link #stop()} needs to be called explicitly in order to release all the resources (e.g. threads,
 * TCP connections).</p>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RemoteCacheManager implements RemoteCacheContainer, Closeable, RemoteCacheManagerMXBean {

   private static final Log log = LogFactory.getLog(RemoteCacheManager.class);

   public static final String HOTROD_CLIENT_PROPERTIES = "hotrod-client.properties";
   public static final String JSON_STRING_ARRAY_ELEMENT_REGEX = "(?:\")([^\"]*)(?:\",?)";

   private volatile boolean started = false;
   private final Map<RemoteCacheKey, RemoteCacheHolder> cacheName2RemoteCache = new HashMap<>();
   private final MarshallerRegistry marshallerRegistry = new MarshallerRegistry();
   private final Configuration configuration;

   private Marshaller marshaller;
   protected ChannelFactory channelFactory;
   protected ClientListenerNotifier listenerNotifier;
   private final Runnable start = this::start;
   private final Runnable stop = this::stop;
   private final RemoteCounterManager counterManager;
   private final TransactionTable syncTransactionTable;
   private final XaModeTransactionTable xaTransactionTable;
   private ObjectName mbeanObjectName;
   private final TimeService timeService = DefaultTimeService.INSTANCE;
   private ExecutorService asyncExecutorService;

   /**
    * Create a new RemoteCacheManager using the supplied {@link Configuration}. The RemoteCacheManager will be started
    * automatically
    *
    * @param configuration the configuration to use for this RemoteCacheManager
    * @since 5.3
    */
   public RemoteCacheManager(Configuration configuration) {
      this(configuration, true);
   }

   /**
    * Create a new RemoteCacheManager using the supplied URI. The RemoteCacheManager will be started
    * automatically
    *
    * @param uri the URI to use for this RemoteCacheManager
    * @since 11.0
    */
   public RemoteCacheManager(String uri) {
      this(HotRodURI.create(uri));
   }

   /**
    * Create a new RemoteCacheManager using the supplied URI. The RemoteCacheManager will be started
    * automatically
    *
    * @param uri the URI to use for this RemoteCacheManager
    * @since 11.0
    */
   public RemoteCacheManager(URI uri) {
      this(HotRodURI.create(uri));
   }

   private RemoteCacheManager(HotRodURI uri) {
      this(uri.toConfigurationBuilder().build());
   }

   /**
    * Create a new RemoteCacheManager using the supplied {@link Configuration}. The RemoteCacheManager will be started
    * automatically only if the start parameter is true
    *
    * @param configuration the configuration to use for this RemoteCacheManager
    * @param start         whether or not to start the manager on return from the constructor.
    * @since 5.3
    */
   public RemoteCacheManager(Configuration configuration, boolean start) {
      this.configuration = configuration;
      this.counterManager = new RemoteCounterManager();
      this.syncTransactionTable = new SyncModeTransactionTable(configuration.transactionTimeout());
      this.xaTransactionTable = new XaModeTransactionTable(configuration.transactionTimeout());
      registerMBean();
      if (start) start();
   }

   /**
    * @since 5.3
    */
   @Override
   public Configuration getConfiguration() {
      return configuration;
   }

   /**
    * <p>Similar to {@link RemoteCacheManager#RemoteCacheManager(Configuration, boolean)}, but it will try to lookup
    * the config properties in the classpath, in a file named <tt>hotrod-client.properties</tt>. If no properties can be
    * found in the classpath, defaults will be used, attempting to connect to <tt>127.0.0.1:11222</tt></p>
    *
    * <p>Refer to
    * {@link ConfigurationBuilder} for a detailed list of available properties.</p>
    *
    * @param start whether or not to start the RemoteCacheManager
    * @throws HotRodClientException if such a file cannot be found in the classpath
    */
   public RemoteCacheManager(boolean start) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      InputStream stream = FileLookupFactory.newInstance().lookupFile(HOTROD_CLIENT_PROPERTIES, cl);
      if (stream == null) {
         HOTROD.couldNotFindPropertiesFile(HOTROD_CLIENT_PROPERTIES);
      } else {
         try {
            builder.withProperties(loadFromStream(stream));
         } finally {
            Util.close(stream);
         }
      }
      this.configuration = builder.build();
      this.counterManager = new RemoteCounterManager();
      this.syncTransactionTable = new SyncModeTransactionTable(configuration.transactionTimeout());
      this.xaTransactionTable = new XaModeTransactionTable(configuration.transactionTimeout());
      registerMBean();
      if (start) actualStart();
   }

   /**
    * Same as {@link #RemoteCacheManager(boolean)} and it also starts the cache.
    */
   public RemoteCacheManager() {
      this(true);
   }

   private void registerMBean() {
      StatisticsConfiguration configuration = this.configuration.statistics();
      if (configuration.jmxEnabled()) {
         try {
            MBeanServer mbeanServer = configuration.mbeanServerLookup().getMBeanServer();
            mbeanObjectName = new ObjectName(String.format("%s:type=HotRodClient,name=%s", configuration.jmxDomain(), configuration.jmxName()));
            mbeanServer.registerMBean(this, mbeanObjectName);
         } catch (Exception e) {
            throw HOTROD.jmxRegistrationFailure(e);
         }
      }
   }

   private void unregisterMBean() {
      if (mbeanObjectName != null) {
         try {
            MBeanServer mBeanServer = configuration.statistics().mbeanServerLookup().getMBeanServer();
            if (mBeanServer.isRegistered(mbeanObjectName)) {
               mBeanServer.unregisterMBean(mbeanObjectName);
            } else {
               HOTROD.debugf("MBean not registered: %s", mbeanObjectName);
            }
         } catch (Exception e) {
            throw HOTROD.jmxUnregistrationFailure(e);
         }
      }
   }

   /**
    * Retrieves a named cache from the remote server if the cache has been defined, otherwise if the cache name is
    * undefined, it will return null.
    *
    * @param cacheName name of cache to retrieve
    * @return a cache instance identified by cacheName or null if the cache name has not been defined
    */
   @Override
   public <K, V> RemoteCache<K, V> getCache(String cacheName) {
      return getCache(cacheName, configuration.forceReturnValues(), null, null);
   }

   @Override
   public Set<String> getCacheNames() {
      OperationsFactory operationsFactory = new OperationsFactory(channelFactory, listenerNotifier, configuration, 0);
      String names = await(operationsFactory.newAdminOperation("@@cache@names", Collections.emptyMap()).execute());
      Set<String> cacheNames = new TreeSet<>();
      // Simple pattern that matches the result which is represented as a JSON string array, e.g. ["cache1","cache2"]
      Pattern pattern = Pattern.compile(JSON_STRING_ARRAY_ELEMENT_REGEX);
      Matcher matcher = pattern.matcher(names);
      while (matcher.find()) {
         cacheNames.add(matcher.group(1));
      }
      return cacheNames;
   }

   /**
    * Retrieves the default cache from the remote server.
    *
    * @return a remote cache instance that can be used to send requests to the default cache in the server
    */
   @Override
   public <K, V> RemoteCache<K, V> getCache() {
      return getCache(configuration.forceReturnValues());
   }

   @Override
   public <K, V> RemoteCache<K, V> getCache(String cacheName, TransactionMode transactionMode,
                                            TransactionManager transactionManager) {
      return createRemoteCache(cacheName, configuration.forceReturnValues(), transactionMode, transactionManager);
   }

   @Override
   public <K, V> RemoteCache<K, V> getCache(String cacheName, boolean forceReturnValue, TransactionMode transactionMode,
                                            TransactionManager transactionManager) {
      return createRemoteCache(cacheName, forceReturnValue, transactionMode, transactionManager);
   }

   public CompletableFuture<Void> startAsync() {
      // The default async executor service is dedicated for Netty, therefore here we'll use common FJP.
      // TODO: This needs to be fixed at some point to not use an additional thread
      return CompletableFuture.runAsync(start, ForkJoinPool.commonPool());
   }

   public CompletableFuture<Void> stopAsync() {
      // The default async executor service is dedicated for Netty, therefore here we'll use common FJP.
      // TODO: This needs to be fixed at some point to not use an additional thread
      return CompletableFuture.runAsync(stop, ForkJoinPool.commonPool());
   }

   @Override
   public void start() {
      if (!started) {
         actualStart();
      }
   }

   private void actualStart() {
      log.debugf("Starting remote cache manager %x", System.identityHashCode(this));
      channelFactory = createChannelFactory();

      marshallerRegistry.registerMarshaller(BytesOnlyMarshaller.INSTANCE);
      marshallerRegistry.registerMarshaller(new UTF8StringMarshaller());
      marshallerRegistry.registerMarshaller(new JavaSerializationMarshaller(configuration.getClassAllowList()));
      registerProtoStreamMarshaller();

      boolean customMarshallerInstance = true;
      marshaller = configuration.marshaller();
      if (marshaller == null) {
         Class<? extends Marshaller> clazz = configuration.marshallerClass();
         marshaller = marshallerRegistry.getMarshaller(clazz);
         if (marshaller == null) {
            marshaller = Util.getInstance(clazz);
         } else {
            customMarshallerInstance = false;
         }
      }

      if (customMarshallerInstance) {
         if (!configuration.serialAllowList().isEmpty()) {
            marshaller.initialize(configuration.getClassAllowList());
         }

         if (marshaller instanceof ProtoStreamMarshaller) {
            initializeProtoStreamMarshaller((ProtoStreamMarshaller) marshaller);
         }

         // Replace any default marshaller with the same media type
         marshallerRegistry.registerMarshaller(marshaller);
      }

      listenerNotifier = new ClientListenerNotifier(marshaller, channelFactory, configuration);
      ExecutorFactory executorFactory = configuration.asyncExecutorFactory().factory();
      if (executorFactory == null) {
         executorFactory = Util.getInstance(configuration.asyncExecutorFactory().factoryClass());
      }
      asyncExecutorService = executorFactory.getExecutor(configuration.asyncExecutorFactory().properties());
      channelFactory.start(marshaller, asyncExecutorService,
                           listenerNotifier, marshallerRegistry);
      counterManager.start(channelFactory, configuration, listenerNotifier);

      TransactionOperationFactory txOperationFactory = new TransactionOperationFactory(configuration, channelFactory);
      syncTransactionTable.start(txOperationFactory);
      xaTransactionTable.start(txOperationFactory);

      // Print version to help figure client version run
      HOTROD.version(Version.printVersion());

      started = true;
   }

   private void registerProtoStreamMarshaller() {
      try {
         ProtoStreamMarshaller protoMarshaller = new ProtoStreamMarshaller();
         marshallerRegistry.registerMarshaller(protoMarshaller);

         initializeProtoStreamMarshaller(protoMarshaller);
      } catch (NoClassDefFoundError e) {
         // Ignore the error, it the protostream dependency is missing
      }
   }

   private void initializeProtoStreamMarshaller(ProtoStreamMarshaller protoMarshaller) {
      SerializationContext ctx = protoMarshaller.getSerializationContext();

      // Register some useful builtin schemas, which the user can override later.
      registerDefaultSchemas(ctx,
                             "org.infinispan.protostream.types.java.CommonContainerTypesSchema",
                             "org.infinispan.protostream.types.java.CommonTypesSchema");
      registerSerializationContextInitializer(ctx, new UserContextInitializerImpl());

      // Register the configured schemas.
      for (SerializationContextInitializer sci : configuration.getContextInitializers()) {
         registerSerializationContextInitializer(ctx, sci);
      }
   }

   private static void registerSerializationContextInitializer(SerializationContext ctx,
                                                               SerializationContextInitializer sci) {
      sci.registerSchema(ctx);
      sci.registerMarshallers(ctx);
   }

   private static void registerDefaultSchemas(SerializationContext ctx, String... classNames) {
      for (String className : classNames) {
         SerializationContextInitializer sci;
         try {
            Class<?> clazz = Class.forName(className);
            Object instance = clazz.getDeclaredConstructor().newInstance();
            sci = (SerializationContextInitializer) instance;
         } catch (Exception e) {
            log.failedToCreatePredefinedSerializationContextInitializer(className, e);
            continue;
         }
         registerSerializationContextInitializer(ctx, sci);
      }
   }

   public ChannelFactory createChannelFactory() {
      return new ChannelFactory(configuration, new CodecHolder(configuration.version().getCodec()));
   }

   @Override
   public boolean isTransactional(String cacheName) {
      ClientStatistics stats = ClientStatistics.dummyClientStatistics(timeService);
      OperationsFactory factory = createOperationFactory(cacheName, false, stats);
      return checkTransactionSupport(cacheName, factory, log);
   }

   public MarshallerRegistry getMarshallerRegistry() {
      return marshallerRegistry;
   }

   /**
    * Stop the remote cache manager, disconnecting all existing connections. As part of the disconnection, all
    * registered client cache listeners will be removed since client no longer can receive callbacks.
    */
   @Override
   public void stop() {
      if (isStarted()) {
         log.debugf("Stopping remote cache manager %x", System.identityHashCode(this));
         synchronized (cacheName2RemoteCache) {
            for (Map.Entry<RemoteCacheKey, RemoteCacheHolder> cache : cacheName2RemoteCache.entrySet()) {
               cache.getValue().remoteCache().stop();
            }
            cacheName2RemoteCache.clear();
         }
         listenerNotifier.stop();
         counterManager.stop();
         channelFactory.destroy();
      }
      unregisterMBean();
      started = false;
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   @Override
   public boolean switchToCluster(String clusterName) {
      return channelFactory.manualSwitchToCluster(clusterName);
   }

   @Override
   public boolean switchToDefaultCluster() {
      return channelFactory.manualSwitchToCluster(ChannelFactory.DEFAULT_CLUSTER_NAME);
   }

   @Override
   public String getCurrentClusterName() {
      return channelFactory.getCurrentClusterName();
   }

   private Properties loadFromStream(InputStream stream) {
      Properties properties = new Properties();
      try {
         properties.load(stream);
      } catch (IOException e) {
         throw new HotRodClientException("Issues configuring from client hotrod-client.properties", e);
      }
      return properties;
   }

   private RemoteCacheConfiguration findConfiguration(String cacheName) {
      if (configuration.remoteCaches().containsKey(cacheName)) {
         return configuration.remoteCaches().get(cacheName);
      }
      // Search for wildcard configurations
      for (Map.Entry<String, RemoteCacheConfiguration> c : configuration.remoteCaches().entrySet()) {
         String key = c.getKey();
         if (GlobUtils.isGlob(key) && cacheName.matches(GlobUtils.globToRegex(key))) {
            return c.getValue();
         }
      }
      return null;
   }

   private <K, V> RemoteCache<K, V> createRemoteCache(String cacheName, boolean forceReturnValueOverride,
                                                      TransactionMode transactionModeOverride, TransactionManager transactionManagerOverride) {
      RemoteCacheConfiguration cacheConfiguration = findConfiguration(cacheName);
      boolean forceReturnValue = forceReturnValueOverride || (cacheConfiguration != null ? cacheConfiguration.forceReturnValues() : configuration.forceReturnValues());
      RemoteCacheKey key = new RemoteCacheKey(cacheName, forceReturnValue);
      if (cacheName2RemoteCache.containsKey(key)) {
         return cacheName2RemoteCache.get(key).remoteCache();
      }

      OperationsFactory operationsFactory = createOperationFactory(cacheName, forceReturnValue, null);
      PingResponse pingResponse;
      if (started) {
         // Verify if the cache exists on the server first
         pingResponse = await(operationsFactory.newFaultTolerantPingOperation().execute());

         // If ping not successful assume that the cache does not exist
         if (pingResponse.isCacheNotFound()) {
            // We may be able to create it. Don't use RemoteCacheAdmin for this, since it would end up calling this method again
            Map<String, byte[]> params = new HashMap<>(2);
            params.put(RemoteCacheManagerAdminImpl.CACHE_NAME, cacheName.getBytes(HotRodConstants.HOTROD_STRING_CHARSET));
            if (cacheConfiguration != null && cacheConfiguration.templateName() != null) {
               params.put(RemoteCacheManagerAdminImpl.CACHE_TEMPLATE, cacheConfiguration.templateName().getBytes(HotRodConstants.HOTROD_STRING_CHARSET));
            } else if (cacheConfiguration != null && cacheConfiguration.configuration() != null) {
               params.put(RemoteCacheManagerAdminImpl.CACHE_CONFIGURATION, new StringConfiguration(cacheConfiguration.configuration()).toStringConfiguration(cacheName).getBytes(HotRodConstants.HOTROD_STRING_CHARSET));
            } else {
               // We cannot create the cache
               return null;
            }
            // Create and re-ping
            OperationsFactory adminOperationsFactory = new OperationsFactory(channelFactory, listenerNotifier, configuration, 0);
            pingResponse = await(adminOperationsFactory.newAdminOperation("@@cache@getorcreate", params).execute().thenCompose(s -> operationsFactory.newFaultTolerantPingOperation().execute()));
         }
      } else {
         pingResponse = PingResponse.EMPTY;
      }

      TransactionMode transactionMode = getTransactionMode(transactionModeOverride, cacheConfiguration);
      InternalRemoteCache<K, V> remoteCache;
      if (transactionMode == TransactionMode.NONE) {
         remoteCache = createRemoteCache(cacheName);
      } else {
         if (!await(checkTransactionSupport(cacheName, operationsFactory).toCompletableFuture())) {
            throw HOTROD.cacheDoesNotSupportTransactions(cacheName);
         } else {
            TransactionManager transactionManager = getTransactionManager(transactionManagerOverride, cacheConfiguration);
            remoteCache = createRemoteTransactionalCache(cacheName, forceReturnValueOverride,
                  transactionMode == TransactionMode.FULL_XA, transactionMode, transactionManager);
         }
      }

      synchronized (cacheName2RemoteCache) {
         startRemoteCache(remoteCache, forceReturnValue);
         RemoteCacheHolder holder = new RemoteCacheHolder(remoteCache, forceReturnValueOverride);
         remoteCache.resolveStorage(pingResponse.getKeyMediaType(), pingResponse.getValueMediaType(), pingResponse.isObjectStorage());
         cacheName2RemoteCache.putIfAbsent(key, holder);
         return remoteCache;
      }
   }

   private <K, V> InternalRemoteCache<K, V> createRemoteCache(String cacheName) {
      RemoteCacheConfiguration remoteCacheConfiguration = configuration.remoteCaches().get(cacheName);
      NearCacheConfiguration nearCache;
      if (remoteCacheConfiguration != null) {
         nearCache = new NearCacheConfiguration(remoteCacheConfiguration.nearCacheMode(), remoteCacheConfiguration.nearCacheMaxEntries(),
               remoteCacheConfiguration.nearCacheBloomFilter(), null, remoteCacheConfiguration.nearCacheFactory());
      } else {
         Pattern pattern = configuration.nearCache().cacheNamePattern();
         if (pattern == null || pattern.matcher(cacheName).matches()) {
            nearCache = configuration.nearCache();
         } else {
            nearCache = new NearCacheConfiguration(NearCacheMode.DISABLED, -1, false);
         }
      }

      if (nearCache.mode() == NearCacheMode.INVALIDATED) {
         Pattern pattern = nearCache.cacheNamePattern();
         if (pattern == null || pattern.matcher(cacheName).matches()) {
            if (log.isTraceEnabled()) {
               log.tracef("Enabling near-caching for cache '%s'", cacheName);
            }
            NearCacheService<K, V> nearCacheService = createNearCacheService(cacheName, nearCache);
            return InvalidatedNearRemoteCache.delegatingNearCache(
                  new RemoteCacheImpl<>(this, cacheName, timeService, nearCacheService) , nearCacheService);
         }
      }
      return new RemoteCacheImpl<>(this, cacheName, timeService);
   }

   protected <K, V> NearCacheService<K, V> createNearCacheService(String cacheName, NearCacheConfiguration cfg) {
      return NearCacheService.create(cfg, listenerNotifier);
   }

   private void startRemoteCache(InternalRemoteCache<?, ?> remoteCache, boolean forceReturnValue) {
      OperationsFactory operationsFactory = createOperationFactory(remoteCache.getName(),
            forceReturnValue, remoteCache.clientStatistics());
      initRemoteCache(remoteCache, operationsFactory);
      remoteCache.start();
   }

   // Method that handles cache initialization - needed as a placeholder
   private void initRemoteCache(InternalRemoteCache<?, ?> remoteCache, OperationsFactory operationsFactory) {
      if (configuration.statistics().jmxEnabled()) {
         remoteCache.init(operationsFactory, configuration, mbeanObjectName);
      } else {
         remoteCache.init(operationsFactory, configuration);
      }
   }

   @Override
   public Marshaller getMarshaller() {
      return marshaller;
   }

   public static byte[] cacheNameBytes(String cacheName) {
      return cacheName.getBytes(HotRodConstants.HOTROD_STRING_CHARSET);
   }

   public static byte[] cacheNameBytes() {
      return HotRodConstants.DEFAULT_CACHE_NAME_BYTES;
   }

   /**
    * Access to administration operations (cache creation, removal, etc)
    *
    * @return an instance of {@link RemoteCacheManagerAdmin} which can perform administrative operations on the server.
    */
   public RemoteCacheManagerAdmin administration() {
      OperationsFactory operationsFactory = new OperationsFactory(channelFactory, listenerNotifier, configuration, 0);
      return new RemoteCacheManagerAdminImpl(this, operationsFactory, EnumSet.noneOf(CacheContainerAdmin.AdminFlag.class),
            name -> {
               synchronized (cacheName2RemoteCache) {
                  // Remove any mappings
                  RemoteCacheHolder removed = cacheName2RemoteCache.remove(new RemoteCacheKey(name, true));
                  if (removed != null) {
                     // stop the remote cache like we do it in the stop() method
                     removed.remoteCache.stop();
                  }
                  removed = cacheName2RemoteCache.remove(new RemoteCacheKey(name, false));
                  if (removed != null) {
                     removed.remoteCache.stop();
                  }
               }
            });
   }

   /**
    * See {@link #stop()}
    */
   @Override
   public void close() {
      stop();
   }

   /**
    * Access to counter operations
    *
    * @return an instance of {@link CounterManager} which can perform counter operations on the server.
    */
   CounterManager getCounterManager() {
      return counterManager;
   }

   /**
    * This method is not a part of the public API. It is exposed for internal purposes only.
    */
   public ChannelFactory getChannelFactory() {
      return channelFactory;
   }

   /**
    * Returns the {@link XAResource} which can be used to do transactional recovery.
    *
    * @return An instance of {@link XAResource}
    */
   public XAResource getXaResource() {
      return xaTransactionTable.getXaResource();
   }

   private TransactionManager getTransactionManager(TransactionManager override, RemoteCacheConfiguration cacheConfiguration) {
      try {
         return override == null ?
               (cacheConfiguration == null ?
                     GenericTransactionManagerLookup.getInstance().getTransactionManager() :
                     cacheConfiguration.transactionManagerLookup().getTransactionManager()) :
               override;
      } catch (Exception e) {
         throw new HotRodClientException(e);
      }
   }

   private TransactionMode getTransactionMode(TransactionMode override, RemoteCacheConfiguration cacheConfiguration) {
      return override == null ?
            (cacheConfiguration == null ? TransactionMode.NONE : cacheConfiguration.transactionMode()) :
            override;
   }

   private TransactionTable getTransactionTable(TransactionMode transactionMode) {
      switch (transactionMode) {
         case NON_XA:
            return syncTransactionTable;
         case NON_DURABLE_XA:
         case FULL_XA:
            return xaTransactionTable;
         default:
            throw new IllegalStateException();
      }
   }

   private <K, V> TransactionalRemoteCacheImpl<K, V> createRemoteTransactionalCache(String cacheName,
                                                                                    boolean forceReturnValues, boolean recoveryEnabled, TransactionMode transactionMode,
                                                                                    TransactionManager transactionManager) {
      return new TransactionalRemoteCacheImpl<>(this, cacheName, forceReturnValues, recoveryEnabled, transactionManager,
            getTransactionTable(transactionMode), timeService);
   }

   /*
    * The following methods are exposed through the MBean
    */
   @Override
   public String[] getServers() {
      Collection<InetSocketAddress> addresses = channelFactory.getServers();
      return addresses.stream().map(socketAddress -> socketAddress.getHostString() + ":" + socketAddress.getPort()).toArray(String[]::new);
   }

   @Override
   public int getActiveConnectionCount() {
      return channelFactory.getNumActive();
   }

   @Override
   public int getConnectionCount() {
      return channelFactory.getNumActive() + channelFactory.getNumIdle();
   }

   @Override
   public int getIdleConnectionCount() {
      return channelFactory.getNumIdle();
   }

   @Override
   public long getRetries() {
      return channelFactory.getRetries();
   }

   OperationsFactory createOperationFactory(String cacheName, boolean forceReturnValue,
                                                   ClientStatistics stats) {
      return createOperationFactory(cacheName, forceReturnValue, stats, 0);
   }

   public OperationsFactory createOperationFactory(String cacheName, boolean forceReturnValue,
                                                    ClientStatistics stats, int flags) {
      return new OperationsFactory(channelFactory, cacheName, forceReturnValue, listenerNotifier, configuration,
            stats, flags);
   }

   public ExecutorService getAsyncExecutorService() {
      return asyncExecutorService;
   }

   private static class RemoteCacheKey {

      final String cacheName;
      final boolean forceReturnValue;

      RemoteCacheKey(String cacheName, boolean forceReturnValue) {
         this.cacheName = cacheName;
         this.forceReturnValue = forceReturnValue;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof RemoteCacheKey)) return false;

         RemoteCacheKey that = (RemoteCacheKey) o;

         if (forceReturnValue != that.forceReturnValue) return false;
         return Objects.equals(cacheName, that.cacheName);
      }

      @Override
      public int hashCode() {
         int result = cacheName != null ? cacheName.hashCode() : 0;
         result = 31 * result + (forceReturnValue ? 1 : 0);
         return result;
      }
   }

   private static class RemoteCacheHolder {
      final InternalRemoteCache<?, ?> remoteCache;
      final boolean forceReturnValue;

      RemoteCacheHolder(InternalRemoteCache<?, ?> remoteCache, boolean forceReturnValue) {
         this.remoteCache = remoteCache;
         this.forceReturnValue = forceReturnValue;
      }

      <K, V> InternalRemoteCache<K, V> remoteCache() {
         return (InternalRemoteCache) remoteCache;
      }
   }
}
