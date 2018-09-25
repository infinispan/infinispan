package org.infinispan.server.hotrod;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JBOSS_MARSHALLING_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN_TYPE;
import static org.infinispan.counter.EmbeddedCounterManagerFactory.asCounterManager;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import javax.security.auth.Subject;
import javax.security.sasl.SaslServerFactory;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.SaslUtils;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.filter.NamedFactory;
import org.infinispan.filter.ParamKeyValueFilterConverterFactory;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.multimap.impl.EmbeddedMultimapCache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.QueryFacade;
import org.infinispan.server.core.ServerConstants;
import org.infinispan.server.core.transport.NettyChannelInitializer;
import org.infinispan.server.core.transport.NettyInitializers;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.hotrod.counter.listener.ClientCounterManagerNotificationManager;
import org.infinispan.server.hotrod.event.KeyValueWithPreviousEventConverterFactory;
import org.infinispan.server.hotrod.iteration.DefaultIterationManager;
import org.infinispan.server.hotrod.iteration.IterationManager;
import org.infinispan.server.hotrod.logging.HotRodAccessLogging;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.transport.TimeoutEnabledChannelInitializer;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.infinispan.util.KeyValuePair;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Hot Rod server, in charge of defining its encoder/decoder and, if clustered, update the topology information on
 * startup and shutdown.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public class HotRodServer extends AbstractProtocolServer<HotRodServerConfiguration> {
   private static final Log log = LogFactory.getLog(HotRodServer.class, Log.class);

   private static final long MILLISECONDS_IN_30_DAYS = TimeUnit.DAYS.toMillis(30);

   private static final Flag[] LOCAL_NON_BLOCKING_GET = new Flag[]{Flag.CACHE_MODE_LOCAL, Flag.SKIP_CACHE_LOAD};


   public HotRodServer() {
      super("HotRod");
   }

   private Address clusterAddress;
   private ServerAddress address;
   private Cache<Address, ServerAddress> addressCache;
   private Map<String, AdvancedCache> knownCaches = CollectionFactory.makeConcurrentMap(4, 0.9f, 16);
   private Map<String, Configuration> knownCacheConfigurations = CollectionFactory.makeConcurrentMap(4, 0.9f, 16);
   private Map<String, ComponentRegistry> knownCacheRegistries = CollectionFactory.makeConcurrentMap(4, 0.9f, 16);
   private final Map<String, CacheInfo> cacheInfo = new ConcurrentHashMap<>();
   private QueryFacade queryFacade;
   private Map<String, SaslServerFactory> saslMechFactories = CollectionFactory.makeConcurrentMap(4, 0.9f, 16);
   private ClientListenerRegistry clientListenerRegistry;
   private Marshaller marshaller;
   private DefaultExecutorService distributedExecutorService;
   private CrashedMemberDetectorListener viewChangeListener;
   private ReAddMyAddressListener topologyChangeListener;
   private ExecutorService executor;
   private IterationManager iterationManager;
   private RemoveCacheListener removeCacheListener;
   private ClientCounterManagerNotificationManager clientCounterNotificationManager;
   private HotRodAccessLogging accessLogging = new HotRodAccessLogging();
   static final KeyValuePair<String, String> UNKNOWN_TYPES = new KeyValuePair<>(APPLICATION_UNKNOWN_TYPE, APPLICATION_UNKNOWN_TYPE);

   public ServerAddress getAddress() {
      return address;
   }

   public Marshaller getMarshaller() {
      return marshaller;
   }

   byte[] query(AdvancedCache<byte[], byte[]> cache, byte[] query) {
      return queryFacade.query(cache, query);
   }

   public ClientListenerRegistry getClientListenerRegistry() {
      return clientListenerRegistry;
   }

   public ClientCounterManagerNotificationManager getClientCounterNotificationManager() {
      return clientCounterNotificationManager;
   }

   @Override
   public ChannelOutboundHandler getEncoder() {
      return null;
   }

   @Override
   public HotRodDecoder getDecoder() {
      return new HotRodDecoder(cacheManager, getExecutor(getQualifiedName()), this);
   }

   /**
    * Class used to create to empty filter converters that ignores marshalling of keys and values
    */
   class ToEmptyBytesFactory implements ParamKeyValueFilterConverterFactory {
      @Override
      public KeyValueFilterConverter getFilterConverter(Object[] params) {
         return ToEmptyBytesKeyValueFilterConverter.INSTANCE;
      }

      @Override
      public boolean binaryParam() {
         // No reason to unmarshall keys/values as we just ignore them anyways
         return true;
      }
   }

   /**
    * Class used to allow for remote clients to essentially ignore the value by returning an empty byte[].
    */
   @SerializeWith(value = ToEmptyBytesKeyValueFilterConverter.ToEmptyBytesKeyValueFilterConverterExternalizer.class)
   static class ToEmptyBytesKeyValueFilterConverter extends AbstractKeyValueFilterConverter {
      private ToEmptyBytesKeyValueFilterConverter() {
      }

      public static ToEmptyBytesKeyValueFilterConverter INSTANCE = new ToEmptyBytesKeyValueFilterConverter();

      static final byte[] bytes = new byte[0];

      @Override
      public Object filterAndConvert(Object key, Object value, Metadata metadata) {
         return bytes;
      }

      @Override
      public MediaType format() {
         return null;
      }

      public static final class ToEmptyBytesKeyValueFilterConverterExternalizer implements Externalizer<ToEmptyBytesKeyValueFilterConverter> {

         @Override
         public void writeObject(ObjectOutput output, ToEmptyBytesKeyValueFilterConverter object) {
         }

         @Override
         public ToEmptyBytesKeyValueFilterConverter readObject(ObjectInput input) {
            return INSTANCE;
         }
      }
   }

   @Override
   protected void startInternal(HotRodServerConfiguration configuration, EmbeddedCacheManager cacheManager) {
      // These are also initialized by super.startInternal, but we need them before
      this.configuration = configuration;
      this.cacheManager = cacheManager;
      this.iterationManager = new DefaultIterationManager();

      // populate the sasl factories based on the required mechs
      setupSasl();

      // Initialize query-specific stuff
      List<QueryFacade> queryFacades = loadQueryFacades();
      queryFacade = queryFacades.size() > 0 ? queryFacades.get(0) : null;
      clientListenerRegistry = new ClientListenerRegistry(cacheManager.getGlobalComponentRegistry().getComponent(EncoderRegistry.class));
      clientCounterNotificationManager = new ClientCounterManagerNotificationManager(asCounterManager(cacheManager));

      addKeyValueFilterConverterFactory(ToEmptyBytesKeyValueFilterConverter.class.getName(), new ToEmptyBytesFactory());

      addCacheEventConverterFactory("key-value-with-previous-converter-factory", new KeyValueWithPreviousEventConverterFactory());
      loadFilterConverterFactories(ParamKeyValueFilterConverterFactory.class, this::addKeyValueFilterConverterFactory);
      loadFilterConverterFactories(CacheEventFilterConverterFactory.class, this::addCacheEventFilterConverterFactory);
      loadFilterConverterFactories(CacheEventConverterFactory.class, this::addCacheEventConverterFactory);
      loadFilterConverterFactories(KeyValueFilterConverterFactory.class, this::addKeyValueFilterConverterFactory);

      removeCacheListener = new RemoveCacheListener();
      cacheManager.addListener(removeCacheListener);

      // Start default cache and the endpoint before adding self to
      // topology in order to avoid topology updates being used before
      // endpoint is available.
      super.startInternal(configuration, cacheManager);

      // Add self to topology cache last, after everything is initialized
      if (Configurations.isClustered(cacheManager.getCacheManagerConfiguration())) {
         defineTopologyCacheConfig(cacheManager);
         if (log.isDebugEnabled())
            log.debugf("Externally facing address is %s:%d", configuration.proxyHost(), configuration.proxyPort());

         addSelfToTopologyView(cacheManager);
      }
   }

   private AbortPolicy abortPolicy = new AbortPolicy() {
      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
         if (executor.isShutdown())
            throw new IllegalLifecycleStateException("Server has been stopped");
         else
            super.rejectedExecution(r, e);
      }
   };

   protected ExecutorService getExecutor(String threadPrefix) {
      if (this.executor == null || this.executor.isShutdown()) {
         DefaultThreadFactory factory = new DefaultThreadFactory(threadPrefix + "-ServerHandler");
         int workerThreads = getWorkerThreads();
         this.executor = new ThreadPoolExecutor(
               workerThreads,
               workerThreads,
               0L, TimeUnit.MILLISECONDS,
               new LinkedBlockingQueue<>(),
               factory,
               abortPolicy);
      }
      return executor;
   }

   @Override
   public ChannelInitializer<Channel> getInitializer() {
      if (configuration.idleTimeout() > 0)
         return new NettyInitializers(new NettyChannelInitializer(this, transport, getEncoder(), getDecoder()), new TimeoutEnabledChannelInitializer<>(this));
      else // Idle timeout logic is disabled with -1 or 0 values
         return new NettyInitializers(new NettyChannelInitializer(this, transport, getEncoder(), getDecoder()));
   }

   private <T> void loadFilterConverterFactories(Class<T> c, BiConsumer<String, T> biConsumer) {
      ServiceFinder.load(c).forEach(factory -> {
         NamedFactory annotation = factory.getClass().getAnnotation(NamedFactory.class);
         if (annotation != null) {
            String name = annotation.name();
            biConsumer.accept(name, factory);
         }
      });
   }

   private List<QueryFacade> loadQueryFacades() {
      List<QueryFacade> facades = new ArrayList<>();
      ServiceLoader.load(QueryFacade.class, getClass().getClassLoader()).forEach(facades::add);
      return facades;
   }

   @Override
   protected void startTransport() {
      // Start predefined caches
      preStartCaches();

      super.startTransport();
   }

   @Override
   protected void startDefaultCache() {
      getCacheInstance(UNKNOWN_TYPES, null, configuration.defaultCacheName(), cacheManager, true, true);
   }

   private void preStartCaches() {
      // Start defined caches to avoid issues with lazily started caches. Skip internal caches if authorization is not
      // enabled
      InternalCacheRegistry icr = cacheManager.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      boolean authz = cacheManager.getCacheManagerConfiguration().security().authorization().enabled();
      for (String cacheName : cacheManager.getCacheNames()) {
         getCacheInstance(UNKNOWN_TYPES, null, cacheName, cacheManager, false, (!icr.internalCacheHasFlag(cacheName, InternalCacheRegistry.Flag.PROTECTED) || authz));
      }
   }

   private void addSelfToTopologyView(EmbeddedCacheManager cacheManager) {
      addressCache = cacheManager.getCache(configuration.topologyCacheName());
      clusterAddress = cacheManager.getAddress();
      address = new ServerAddress(configuration.publicHost(), configuration.publicPort());
      distributedExecutorService = new DefaultExecutorService(addressCache);

      viewChangeListener = new CrashedMemberDetectorListener(addressCache, this);
      cacheManager.addListener(viewChangeListener);
      topologyChangeListener = new ReAddMyAddressListener(addressCache, clusterAddress, address);
      addressCache.addListener(topologyChangeListener);

      // Map cluster address to server endpoint address
      log.debugf("Map %s cluster address with %s server endpoint in address cache", clusterAddress, address);
      // Guaranteed delivery required since if data is lost, there won't be
      // any further cache calls, so negative acknowledgment can cause issues.
      addressCache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD, Flag.GUARANTEED_DELIVERY)
            .put(clusterAddress, address);
   }

   private void defineTopologyCacheConfig(EmbeddedCacheManager cacheManager) {
      InternalCacheRegistry internalCacheRegistry = cacheManager.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      internalCacheRegistry.registerInternalCache(configuration.topologyCacheName(),
            createTopologyCacheConfig(cacheManager.getCacheManagerConfiguration().transport().distributedSyncTimeout()).build(),
            EnumSet.of(InternalCacheRegistry.Flag.EXCLUSIVE));
   }

   protected ConfigurationBuilder createTopologyCacheConfig(long distSyncTimeout) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.REPL_SYNC).remoteTimeout(configuration.topologyReplTimeout())
            .locking().lockAcquisitionTimeout(configuration.topologyLockTimeout())
            .clustering().partitionHandling().mergePolicy(null)
            .expiration().lifespan(-1).maxIdle(-1);

      if (configuration.topologyStateTransfer()) {
         builder
               .clustering()
               .stateTransfer()
               .awaitInitialTransfer(configuration.topologyAwaitInitialTransfer())
               .fetchInMemoryState(true)
               .timeout(distSyncTimeout + configuration.topologyReplTimeout());
      } else {
         builder.persistence().addClusterLoader().remoteCallTimeout(configuration.topologyReplTimeout());
      }

      return builder;
   }

   public AdvancedCache<byte[], byte[]> cache(HotRodHeader header, Subject subject) throws RequestParsingException {
      return cache(header, subject, header.cacheName);
   }

   public AdvancedCache<byte[], byte[]> cache(HotRodHeader header, Subject subject, String cacheName) throws RequestParsingException {
      if (isCacheIgnored(cacheName)) {
         throw new CacheUnavailableException();
      }
      // Try to avoid calling cacheManager.getCacheNames() if possible, since this creates a lot of unnecessary garbage
      KeyValuePair<String, String> requestMediaTypes = getRequestMediaTypes(header, getCacheConfiguration(cacheName));
      AdvancedCache<byte[], byte[]> cache = knownCaches.get(getDecoratedCacheKey(cacheName, requestMediaTypes));
      if (cache == null) {
         InternalCacheRegistry icr = SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(InternalCacheRegistry.class);
         if (icr.isPrivateCache(cacheName)) {
            throw new RequestParsingException(
                  String.format("Remote requests are not allowed to private caches. Do no send remote requests to cache '%s'", cacheName),
                  header.version, header.messageId);
         } else if (icr.internalCacheHasFlag(cacheName, InternalCacheRegistry.Flag.PROTECTED)) {
            // We want to make sure the cache access is checked everytime, so don't store it as a "known" cache. More
            // expensive, but these caches should not be accessed frequently
            cache = getCacheInstance(requestMediaTypes, header, cacheName, cacheManager, true, false);
         } else if (!cacheName.isEmpty() && !cacheManager.getCacheNames().contains(cacheName)) {
            throw new CacheNotFoundException(
                  String.format("Cache with name '%s' not found amongst the configured caches", cacheName),
                  header.version, header.messageId);
         } else {
            cache = getCacheInstance(requestMediaTypes, header, cacheName, cacheManager, true, true);
         }
      }
      cache = header.getOptimizedCache(cache, getCacheConfiguration(cacheName));
      if (subject != null) {
         cache = cache.withSubject(subject);
      }
      return cache;
   }

   public EmbeddedMultimapCache<WrappedByteArray, WrappedByteArray> multimap(HotRodHeader header, Subject subject) {
      return new EmbeddedMultimapCache(cache(header, subject));
   }

   public void cacheStopped(String cacheName) {
      cacheInfo.keySet().stream().filter(k -> k.startsWith(cacheName)).forEach(cacheInfo::remove);
   }

   public CacheInfo getCacheInfo(AdvancedCache<byte[], byte[]> cache, HotRodHeader header) {
      // Fetching persistence manager would require security action, and would be too expensive
      CacheInfo info = cacheInfo.get(cache.getName() + header.getKeyMediaType().getTypeSubtype() + header.getValueMediaType().getTypeSubtype());
      if (info == null) {
         AdvancedCache<byte[], byte[]> localNonBlocking = SecurityActions.anonymizeSecureCache(cache)
               .noFlags().withFlags(LOCAL_NON_BLOCKING_GET);
         if (cache.getStatus() != ComponentStatus.RUNNING) {
            // stay on the safe side
            return new CacheInfo(localNonBlocking, true, true);
         }
         ComponentRegistry cr = SecurityActions.getCacheComponentRegistry(cache);
         PersistenceManager pm = cr.getComponent(PersistenceManager.class);
         boolean hasIndexing = SecurityActions.getCacheConfiguration(cache).indexing().index().isEnabled();
         info = new CacheInfo(localNonBlocking, pm.isEnabled(), hasIndexing);
         cacheInfo.put(cache.getName() + header.getKeyMediaType().getTypeSubtype() + header.getValueMediaType().getTypeSubtype(), info);
      }
      return info;
   }

   AdvancedCache getCacheInstance(KeyValuePair<String, String> requestTypes, HotRodHeader header, String cacheName, EmbeddedCacheManager cacheManager, Boolean skipCacheCheck, Boolean addToKnownCaches) {
      AdvancedCache cache = null;
      String scopedCacheKey = getDecoratedCacheKey(cacheName, requestTypes);
      if (!skipCacheCheck) cache = knownCaches.get(scopedCacheKey);

      if (cache == null) {
         String validCacheName = cacheName.isEmpty() ? configuration.defaultCacheName() : cacheName;
         cache = SecurityActions.getCache(cacheManager, validCacheName).getAdvancedCache();
         Configuration cacheConfiguration = SecurityActions.getCacheConfiguration(cache);
         // We don't need synchronization as long as we store the cache last
         knownCacheConfigurations.put(cacheName, cacheConfiguration);
         knownCacheRegistries.put(cacheName, SecurityActions.getCacheComponentRegistry(cache.getAdvancedCache()));
         cache = cache.withMediaType(requestTypes.getKey(), requestTypes.getValue());
         if (addToKnownCaches) {
            knownCaches.put(scopedCacheKey, cache);
         }
         // make sure we register a Migrator for this cache!
         tryRegisterMigrationManager(cache);
      }

      return cache;
   }

   Configuration getCacheConfiguration(String cacheName) {
      return knownCacheConfigurations.get(cacheName);
   }

   ComponentRegistry getCacheRegistry(String cacheName) {
      return knownCacheRegistries.get(cacheName);
   }

   private void tryRegisterMigrationManager(AdvancedCache<byte[], byte[]> cache) {
      ComponentRegistry cr = SecurityActions.getCacheComponentRegistry(cache.getAdvancedCache());
      RollingUpgradeManager migrationManager = cr.getComponent(RollingUpgradeManager.class);
      if (migrationManager != null) migrationManager.addSourceMigrator(new HotRodSourceMigrator(cache));
   }

   private void setupSasl() {
      Iterator<SaslServerFactory> saslFactories = SaslUtils.getSaslServerFactories(this.getClass().getClassLoader(), true);
      while (saslFactories.hasNext()) {
         SaslServerFactory saslFactory = saslFactories.next();
         String[] saslFactoryMechs = saslFactory.getMechanismNames(configuration.authentication().mechProperties());
         for (String supportedMech : saslFactoryMechs) {
            for (String mech : configuration.authentication().allowedMechs()) {
               if (supportedMech.equals(mech)) {
                  saslMechFactories.putIfAbsent(mech, saslFactory);
               }
            }
         }
      }
   }

   SaslServerFactory getSaslServerFactory(String mech) {
      return saslMechFactories.get(mech);
   }

   public Cache<Address, ServerAddress> getAddressCache() {
      return addressCache;
   }

   public void addCacheEventFilterFactory(String name, CacheEventFilterFactory factory) {
      clientListenerRegistry.addCacheEventFilterFactory(name, factory);
   }

   public void removeCacheEventFilterFactory(String name) {
      clientListenerRegistry.removeCacheEventFilterFactory(name);
   }

   public void addCacheEventConverterFactory(String name, CacheEventConverterFactory factory) {
      clientListenerRegistry.addCacheEventConverterFactory(name, factory);
   }

   public void removeCacheEventConverterFactory(String name) {
      clientListenerRegistry.removeCacheEventConverterFactory(name);
   }

   public void addCacheEventFilterConverterFactory(String name, CacheEventFilterConverterFactory factory) {
      clientListenerRegistry.addCacheEventFilterConverterFactory(name, factory);
   }

   public void removeCacheEventFilterConverterFactory(String name) {
      clientListenerRegistry.removeCacheEventFilterConverterFactory(name);
   }

   public void setMarshaller(Marshaller marshaller) {
      this.marshaller = marshaller;
      Optional<Marshaller> optMarshaller = Optional.ofNullable(marshaller);
      clientListenerRegistry.setEventMarshaller(optMarshaller);
   }

   public void addKeyValueFilterConverterFactory(String name, KeyValueFilterConverterFactory factory) {
      iterationManager.addKeyValueFilterConverterFactory(name, factory);
   }

   public void removeKeyValueFilterConverterFactory(String name) {
      iterationManager.removeKeyValueFilterConverterFactory(name);
   }

   public IterationManager getIterationManager() {
      return iterationManager;
   }

   static String getDecoratedCacheKey(String cacheName, KeyValuePair<String, String> mediaTypes) {
      return cacheName + "|" + mediaTypes.getKey() + "|" + mediaTypes.getValue();
   }

   private static KeyValuePair<String, String> getRequestMediaTypes(HotRodHeader header, Configuration configuration) {
      String keyRequestType = header == null ? APPLICATION_UNKNOWN_TYPE : header.getKeyMediaType().toString();
      String valueRequestType = header == null ? APPLICATION_UNKNOWN_TYPE : header.getValueMediaType().toString();
      if (header != null && HotRodVersion.HOTROD_28.isOlder(header.version)) {
         // Pre-2.8 clients always send protobuf payload to the metadata cache
         if (header.cacheName.equals(PROTOBUF_METADATA_CACHE_NAME)) {
            keyRequestType = APPLICATION_PROTOSTREAM_TYPE;
            valueRequestType = APPLICATION_PROTOSTREAM_TYPE;
         } else {
            // Pre-2.8 clients always sent query encoded as protobuf unless object store is used.
            if (header.op == HotRodOperation.QUERY) {
               boolean objectStorage = APPLICATION_OBJECT.match(configuration.encoding().valueDataType().mediaType());
               keyRequestType = objectStorage ? APPLICATION_JBOSS_MARSHALLING_TYPE : APPLICATION_PROTOSTREAM_TYPE;
               valueRequestType = objectStorage ? APPLICATION_JBOSS_MARSHALLING_TYPE : APPLICATION_PROTOSTREAM_TYPE;
            }
         }
      }
      return new KeyValuePair<>(keyRequestType, valueRequestType);
   }

   @Override
   public void stop() {
      if (removeCacheListener != null) {
         SecurityActions.removeListener(cacheManager, removeCacheListener);
      }
      if (viewChangeListener != null) {
         SecurityActions.removeListener(cacheManager, viewChangeListener);
      }
      if (topologyChangeListener != null) {
         SecurityActions.removeListener(addressCache, topologyChangeListener);
      }
      if (Configurations.isClustered(cacheManager.getCacheManagerConfiguration())) {
         InternalCacheRegistry internalCacheRegistry = cacheManager.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
         if (internalCacheRegistry != null)
            internalCacheRegistry.unregisterInternalCache(configuration.topologyCacheName());
      }
      if (distributedExecutorService != null) {
         distributedExecutorService.shutdownNow();
      }

      if (clientListenerRegistry != null) clientListenerRegistry.stop();
      if (clientCounterNotificationManager != null) clientCounterNotificationManager.stop();
      if (executor != null) executor.shutdownNow();
      super.stop();
   }

   public HotRodAccessLogging accessLogging() {
      return accessLogging;
   }

   public Metadata buildMetadata(HotRodHeader header, long lifespan, TimeUnitValue lifespanUnit, long maxIdle, TimeUnitValue maxIdleUnit) {
      EmbeddedMetadata.Builder metadata = new EmbeddedMetadata.Builder();
      ComponentRegistry cacheRegistry;
      try {
         cacheRegistry = getCacheRegistry(header.cacheName);
         if (cacheRegistry == null) {
            cache(header, null);
            cacheRegistry = getCacheRegistry(header.cacheName);
         }
      } catch (Exception e) {
         // Obtaining a protected/internal cache can result in an exception. However we have to finish reading
         // the request and we can be sure that the exception will be thrown again once the request will be applied
         // on given cache.
         return null;
      }
      metadata.version(generateVersion(cacheRegistry));
      if (lifespan != ServerConstants.EXPIRATION_DEFAULT && lifespanUnit != TimeUnitValue.DEFAULT) {
         if (lifespanUnit == TimeUnitValue.INFINITE) {
            metadata.lifespan(ServerConstants.EXPIRATION_NONE);
         } else {
            metadata.lifespan(toMillis(lifespan, lifespanUnit));
         }
      }
      if (maxIdle != ServerConstants.EXPIRATION_DEFAULT && maxIdleUnit != TimeUnitValue.DEFAULT) {
         if (maxIdleUnit == TimeUnitValue.INFINITE) {
            metadata.maxIdle(ServerConstants.EXPIRATION_NONE);
         } else {
            metadata.maxIdle(toMillis(maxIdle, maxIdleUnit));
         }
      }
      return metadata.build();
   }

   private EntryVersion generateVersion(ComponentRegistry registry) {
      VersionGenerator cacheVersionGenerator = registry.getVersionGenerator();
      return cacheVersionGenerator.generateNew();
   }

   /**
    * Transforms lifespan pass as seconds into milliseconds following this rule (inspired by Memcached):
    * <p>
    * If lifespan is bigger than number of seconds in 30 days, then it is considered unix time. After converting it to
    * milliseconds, we subtract the current time in and the result is returned.
    * <p>
    * Otherwise it's just considered number of seconds from now and it's returned in milliseconds unit.
    */
   private static long toMillis(long duration, TimeUnitValue unit) {
      if (duration > 0) {
         long milliseconds = unit.toTimeUnit().toMillis(duration);
         if (milliseconds > MILLISECONDS_IN_30_DAYS) {
            long unixTimeExpiry = milliseconds - System.currentTimeMillis();
            return unixTimeExpiry < 0 ? 0 : unixTimeExpiry;
         } else {
            return milliseconds;
         }
      } else {
         return duration;
      }
   }

   @Override
   public int getWorkerThreads() {
      return Integer.getInteger("infinispan.server.hotrod.workerThreads", configuration.workerThreads());
   }

   public static class CacheInfo {
      final AdvancedCache<byte[], byte[]> localNonBlocking;
      final boolean persistence;
      final boolean indexing;

      CacheInfo(AdvancedCache<byte[], byte[]> localNonBlocking, boolean persistence, boolean indexing) {
         this.localNonBlocking = localNonBlocking;
         this.persistence = persistence;
         this.indexing = indexing;
      }

      AdvancedCache<byte[], byte[]> localNonBlocking(Subject subject) {
         if (subject == null) {
            return localNonBlocking;
         } else {
            return localNonBlocking.withSubject(subject);
         }
      }
   }

   @Listener(sync = false, observation = Listener.Observation.POST)
   class ReAddMyAddressListener {
      private final Cache<Address, ServerAddress> addressCache;
      private final Address clusterAddress;
      private final ServerAddress address;

      ReAddMyAddressListener(Cache<Address, ServerAddress> addressCache, Address clusterAddress, ServerAddress address) {
         this.addressCache = addressCache;
         this.clusterAddress = clusterAddress;
         this.address = address;
      }

      @TopologyChanged
      public void topologyChanged(TopologyChangedEvent<Address, ServerAddress> event) {
         boolean success = false;
         while (!success && !distributedExecutorService.isShutdown() && addressCache.getStatus().allowInvocations()) {
            try {
               List<CompletableFuture<Boolean>> futures = distributedExecutorService.submitEverywhere(
                     new CheckAddressTask(clusterAddress));
               // No need for a timeout here, the distributed executor has a default task timeout
               AtomicBoolean result = new AtomicBoolean(true);
               futures.forEach(f -> {
                  try {
                     if (!f.get()) {
                        result.set(false);
                     }
                  } catch (InterruptedException | ExecutionException e) {
                     throw new CacheException(e);
                  }
               });
               if (!result.get()) {
                  log.debugf("Re-adding %s to the topology cache", clusterAddress);
                  addressCache.putAsync(clusterAddress, address);
               }
               success = true;
            } catch (Throwable e) {
               log.debug("Error re-adding address to topology cache, retrying", e);
            }
         }
      }
   }

   @Listener
   class RemoveCacheListener {
      @CacheStopped
      public void cacheStopped(CacheStoppedEvent event) {
         knownCaches.keySet().stream().filter(k -> k.startsWith(event.getCacheName() + "|")).forEach(knownCaches::remove);
         knownCacheConfigurations.remove(event.getCacheName());
         knownCacheRegistries.remove(event.getCacheName());
      }
   }
}

class CheckAddressTask implements DistributedCallable<Address, ServerAddress, Boolean>, Serializable {
   private final Address clusterAddress;

   private volatile Cache<Address, ServerAddress> cache = null;

   CheckAddressTask(Address clusterAddress) {
      this.clusterAddress = clusterAddress;
   }

   @Override
   public void setEnvironment(Cache<Address, ServerAddress> cache, Set<Address> inputKeys) {
      this.cache = cache;
   }

   @Override
   public Boolean call() throws Exception {
      return cache.containsKey(clusterAddress);
   }
}
