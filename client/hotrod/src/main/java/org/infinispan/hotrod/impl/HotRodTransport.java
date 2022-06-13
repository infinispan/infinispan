package org.infinispan.hotrod.impl;

import static org.infinispan.hotrod.impl.Util.await;
import static org.infinispan.hotrod.impl.Util.checkTransactionSupport;
import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.transaction.TransactionManager;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.commons.marshall.UserContextInitializerImpl;
import org.infinispan.commons.time.DefaultTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.GlobUtils;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.hotrod.configuration.HotRodConfiguration;
import org.infinispan.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.hotrod.configuration.RemoteCacheConfiguration;
import org.infinispan.hotrod.configuration.TransactionMode;
import org.infinispan.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.hotrod.exceptions.HotRodClientException;
import org.infinispan.hotrod.impl.cache.ClientStatistics;
import org.infinispan.hotrod.impl.cache.InvalidatedNearRemoteCache;
import org.infinispan.hotrod.impl.cache.MBeanHelper;
import org.infinispan.hotrod.impl.cache.RemoteCache;
import org.infinispan.hotrod.impl.cache.RemoteCacheImpl;
import org.infinispan.hotrod.impl.counter.RemoteCounterManager;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.hotrod.impl.operations.PingResponse;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transaction.SyncModeTransactionTable;
import org.infinispan.hotrod.impl.transaction.TransactionOperationFactory;
import org.infinispan.hotrod.impl.transaction.TransactionTable;
import org.infinispan.hotrod.impl.transaction.TransactionalRemoteCacheImpl;
import org.infinispan.hotrod.impl.transaction.XaModeTransactionTable;
import org.infinispan.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.hotrod.marshall.BytesOnlyMarshaller;
import org.infinispan.hotrod.near.NearCacheService;
import org.infinispan.hotrod.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;

/**
 * @since 14.0
 **/
public class HotRodTransport implements AutoCloseable {
   private final static Log log = LogFactory.getLog(HotRodTransport.class);
   private static final String JSON_STRING_ARRAY_ELEMENT_REGEX = "(?:\")([^\"]*)(?:\",?)";
   private final MarshallerRegistry marshallerRegistry;
   private final HotRodConfiguration configuration;
   private final RemoteCounterManager counterManager;
   private final TransactionTable syncTransactionTable;
   private final XaModeTransactionTable xaTransactionTable;
   private final ChannelFactory channelFactory;
   private final Codec codec;
   private final ExecutorService asyncExecutorService;
   private final Marshaller marshaller;
   private ClientListenerNotifier listenerNotifier;
   private TimeService timeService = DefaultTimeService.INSTANCE;
   private volatile boolean started = false;
   private final ConcurrentMap<RemoteCacheKey, CompletionStage<RemoteCache<Object, Object>>> cacheName2RemoteCache = new ConcurrentHashMap<>();
   private final MBeanHelper mBeanHelper;

   public HotRodTransport(HotRodConfiguration configuration) {
      this.configuration = configuration;
      counterManager = new RemoteCounterManager();
      syncTransactionTable = new SyncModeTransactionTable(configuration.transactionTimeout());
      xaTransactionTable = new XaModeTransactionTable(configuration.transactionTimeout());
      channelFactory = new ChannelFactory();
      codec = Codec.forProtocol(configuration.version());

      marshallerRegistry = new MarshallerRegistry();
      marshallerRegistry.registerMarshaller(BytesOnlyMarshaller.INSTANCE);
      marshallerRegistry.registerMarshaller(new UTF8StringMarshaller());
      marshallerRegistry.registerMarshaller(new JavaSerializationMarshaller(configuration.getClassAllowList()));
      try {
         ProtoStreamMarshaller protoMarshaller = new ProtoStreamMarshaller();
         marshallerRegistry.registerMarshaller(protoMarshaller);

         initProtoStreamMarshaller(protoMarshaller);
      } catch (NoClassDefFoundError e) {
         // Ignore the error, if the protostream dependency is missing
      }
      marshaller = initMarshaller();
      ExecutorFactory executorFactory = configuration.asyncExecutorFactory().factory();
      if (executorFactory == null) {
         executorFactory = Util.getInstance(configuration.asyncExecutorFactory().factoryClass());
      }
      asyncExecutorService = executorFactory.getExecutor(configuration.asyncExecutorFactory().properties());
      mBeanHelper = MBeanHelper.getInstance(this);
   }

   private Marshaller initMarshaller() {
      boolean customMarshallerInstance = true;
      Marshaller marshaller = configuration.marshaller();
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
         if (configuration.serialAllowList().length == 0) {
            marshaller.initialize(configuration.getClassAllowList());
         }
         if (marshaller instanceof ProtoStreamMarshaller) {
            initProtoStreamMarshaller((ProtoStreamMarshaller) marshaller);
         }
         // Replace any default marshaller with the same media type
         marshallerRegistry.registerMarshaller(marshaller);
      }
      return marshaller;
   }

   public MBeanHelper getMBeanHelper() {
      return mBeanHelper;
   }

   private void initProtoStreamMarshaller(ProtoStreamMarshaller protoMarshaller) {
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
            HOTROD.failedToCreatePredefinedSerializationContextInitializer(className, e);
            continue;
         }
         registerSerializationContextInitializer(ctx, sci);
      }
   }

   public HotRodConfiguration getConfiguration() {
      return configuration;
   }

   public TimeService getTimeService() {
      return timeService;
   }

   public MarshallerRegistry getMarshallerRegistry() {
      return marshallerRegistry;
   }

   public ChannelFactory getChannelFactory() {
      return channelFactory;
   }

   public Marshaller getMarshaller() {
      return marshaller;
   }

   public CounterManager getCounterManager() {
      return counterManager;
   }

   public Codec getCodec() {
      return codec;
   }

   public XaModeTransactionTable getXaTransactionTable() {
      return xaTransactionTable;
   }

   public TransactionTable getTransactionTable(TransactionMode transactionMode) {
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

   public <K, V> NearCacheService<K, V> createNearCacheService(String cacheName, NearCacheConfiguration cfg) {
      return NearCacheService.create(cfg, listenerNotifier);
   }

   public CacheOperationsFactory createCacheOperationFactory(String cacheName, ClientStatistics stats) {
      return new CacheOperationsFactory(channelFactory, cacheName, codec, listenerNotifier, configuration, stats);
   }

   public void start() {
      if (!started) {
         HOTROD.debugf("Starting Hot Rod client %x", System.identityHashCode(this));
         channelFactory.start(codec, configuration, marshaller, asyncExecutorService,
               listenerNotifier, marshallerRegistry);
         counterManager.start(channelFactory, codec, configuration, listenerNotifier);
         listenerNotifier = new ClientListenerNotifier(codec, channelFactory, configuration);
         TransactionOperationFactory txOperationFactory = new TransactionOperationFactory(configuration, channelFactory, codec);
         syncTransactionTable.start(txOperationFactory);
         xaTransactionTable.start(txOperationFactory);
         HOTROD.debugf("Infinispan version: %s", Version.printVersion());
         started = true;
      }
   }

   @Override
   public void close() {
      if (started) {
         listenerNotifier.stop();
         counterManager.stop();
         channelFactory.destroy();
         started = false;
      }
      mBeanHelper.close();
   }

   public boolean isStarted() {
      return started;
   }

   public <K, V> CompletionStage<RemoteCache<K, V>> getRemoteCache(String cacheName) {
      RemoteCacheConfiguration cacheConfiguration = findConfiguration(cacheName);
      return getRemoteCache(cacheName, cacheConfiguration);
   }

   public <K, V> CompletionStage<RemoteCache<K, V>> getRemoteCache(String cacheName, RemoteCacheConfiguration cacheConfiguration) {
      boolean forceReturnValue = (cacheConfiguration != null ? cacheConfiguration.forceReturnValues() : configuration.forceReturnValues());
      RemoteCacheKey key = new RemoteCacheKey(cacheName, forceReturnValue);

      CompletionStage<RemoteCache<Object, Object>> remoteCache = cacheName2RemoteCache.computeIfAbsent(key, k ->
            pingRemoteCache(cacheName, cacheConfiguration)
      );
      return (CompletionStage) remoteCache;
   }

   private <K, V> CompletionStage<RemoteCache<K, V>> pingRemoteCache(String cacheName, RemoteCacheConfiguration cacheConfiguration) {
      CacheOperationsFactory cacheOperationsFactory = createOperationFactory(cacheName, codec, null);
      CompletionStage<PingResponse> pingResponse;
      if (started) {
         // Verify if the cache exists on the server first
         pingResponse = cacheOperationsFactory.newFaultTolerantPingOperation().execute();
         pingResponse.thenCompose(ping -> {
            if (ping.isCacheNotFound()) {
               return createRemoteCache(cacheOperationsFactory, cacheName, cacheConfiguration);
            } else {
               return pingResponse;
            }
         });
      } else {
         pingResponse = CompletableFuture.completedFuture(PingResponse.EMPTY);
      }
      return pingResponse.thenApply(ping -> {
         TransactionMode transactionMode = cacheConfiguration != null ? cacheConfiguration.transactionMode() : TransactionMode.NONE;
         RemoteCache<K, V> remoteCache;
         if (transactionMode == TransactionMode.NONE) {
            if (cacheConfiguration != null && cacheConfiguration.nearCache().mode().enabled()) {
               NearCacheConfiguration nearCache = cacheConfiguration.nearCache();
               if (log.isTraceEnabled()) {
                  log.tracef("Enabling near-caching for cache '%s'", cacheName);
               }
               NearCacheService<K, V> nearCacheService = createNearCacheService(cacheName, nearCache);
               remoteCache = InvalidatedNearRemoteCache.delegatingNearCache(
                     new RemoteCacheImpl<>(this, cacheName, timeService, nearCacheService), nearCacheService);
            } else {
               remoteCache = new RemoteCacheImpl<>(this, cacheName, timeService, null);
            }
         } else {
            if (!await(checkTransactionSupport(cacheName, cacheOperationsFactory).toCompletableFuture())) {
               throw HOTROD.cacheDoesNotSupportTransactions(cacheName);
            } else {
               TransactionManager transactionManager = getTransactionManager(cacheConfiguration);
               remoteCache = createRemoteTransactionalCache(cacheName, transactionMode == TransactionMode.FULL_XA, transactionMode, transactionManager);
            }
         }
         remoteCache.resolveStorage(ping.isObjectStorage());
         return remoteCache;
      });
   }

   private CompletionStage<PingResponse> createRemoteCache(CacheOperationsFactory cacheOperationsFactory, String cacheName, RemoteCacheConfiguration cacheConfiguration) {
      Map<String, byte[]> params = new HashMap<>(2);
      params.put("name", cacheName.getBytes(HotRodConstants.HOTROD_STRING_CHARSET));
      if (cacheConfiguration != null && cacheConfiguration.templateName() != null) {
         params.put("template", cacheConfiguration.templateName().getBytes(HotRodConstants.HOTROD_STRING_CHARSET));
      } else if (cacheConfiguration != null && cacheConfiguration.configuration() != null) {
         params.put("configuration", new StringConfiguration(cacheConfiguration.configuration()).toStringConfiguration(cacheName).getBytes(HotRodConstants.HOTROD_STRING_CHARSET));
      } else {
         // We cannot create the cache
         throw new HotRodClientException("Cache " + cacheName + " does not exist");
      }
      // Create and re-ping
      CacheOperationsFactory adminCacheOperationsFactory = new CacheOperationsFactory(channelFactory, codec, listenerNotifier, configuration);
      return adminCacheOperationsFactory.newAdminOperation("@@cache@getorcreate", params, CacheOptions.DEFAULT).execute().thenCompose(s -> cacheOperationsFactory.newFaultTolerantPingOperation().execute());
   }

   public CompletionStage<Void> removeCache(String cacheName) {
      Map<String, byte[]> params = new HashMap<>(2);
      params.put("name", cacheName.getBytes(HotRodConstants.HOTROD_STRING_CHARSET));
      CacheOperationsFactory adminCacheOperationsFactory = new CacheOperationsFactory(channelFactory, codec, listenerNotifier, configuration);
      return adminCacheOperationsFactory.newAdminOperation("@@cache@remove", params, CacheOptions.DEFAULT).execute().thenApply(s -> null); // TODO: do something with the return message
   }

   public CompletionStage<Set<String>> getCacheNames() {
      return getConfigurationNames("@@cache@names");
   }

   public CompletionStage<Set<String>> getTemplateNames() {
      return getConfigurationNames("@@cache@templates");
   }

   private CompletionStage<Set<String>> getConfigurationNames(String taskName) {
      CacheOperationsFactory adminCacheOperationsFactory = new CacheOperationsFactory(channelFactory, codec, listenerNotifier, configuration);
      return adminCacheOperationsFactory.newAdminOperation(taskName, Collections.emptyMap(), CacheOptions.DEFAULT).execute().thenApply(names -> {
         Set<String> cacheNames = new HashSet<>();
         // Simple pattern that matches the result which is represented as a JSON string array, e.g. ["cache1","cache2"]
         Pattern pattern = Pattern.compile(JSON_STRING_ARRAY_ELEMENT_REGEX);
         Matcher matcher = pattern.matcher(names);
         while (matcher.find()) {
            cacheNames.add(matcher.group(1));
         }
         return cacheNames;
      });
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

   private TransactionManager getTransactionManager(RemoteCacheConfiguration cacheConfiguration) {
      try {
         return (cacheConfiguration == null ?
               GenericTransactionManagerLookup.getInstance().getTransactionManager() :
               cacheConfiguration.transactionManagerLookup().getTransactionManager());
      } catch (Exception e) {
         throw new HotRodClientException(e);
      }
   }

   private <K, V> TransactionalRemoteCacheImpl<K, V> createRemoteTransactionalCache(String cacheName,
                                                                                    boolean recoveryEnabled, TransactionMode transactionMode,
                                                                                    TransactionManager transactionManager) {
      return new TransactionalRemoteCacheImpl<>(this, cacheName, recoveryEnabled, transactionManager,
            getTransactionTable(transactionMode), timeService);
   }

   private CacheOperationsFactory createOperationFactory(String cacheName, Codec codec,
                                                         ClientStatistics stats) {
      return new CacheOperationsFactory(channelFactory, cacheName, codec, listenerNotifier, configuration, stats);
   }

   public static byte[] cacheNameBytes(String cacheName) {
      return cacheName.getBytes(HotRodConstants.HOTROD_STRING_CHARSET);
   }

   public static byte[] cacheNameBytes() {
      return HotRodConstants.DEFAULT_CACHE_NAME_BYTES;
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
}
