package org.infinispan.server.hotrod;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JBOSS_MARSHALLING;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.counter.EmbeddedCounterManagerFactory.asCounterManager;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.security.auth.Subject;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.filter.NamedFactory;
import org.infinispan.filter.ParamKeyValueFilterConverterFactory;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.multimap.impl.EmbeddedMultimapCache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheEntryListenerInvocation;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.CacheNotifierImpl;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
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

   public static final int LISTENERS_CHECK_INTERVAL = 10;

   private boolean hasDefaultCache;
   private Address clusterAddress;
   private ServerAddress address;
   private Cache<Address, ServerAddress> addressCache;
   private final Map<String, CacheInfo> knownCaches = new ConcurrentHashMap<>();
   private QueryFacade queryFacade;
   private ClientListenerRegistry clientListenerRegistry;
   private Marshaller marshaller;
   private ClusterExecutor clusterExecutor;
   private CrashedMemberDetectorListener viewChangeListener;
   private ReAddMyAddressListener topologyChangeListener;
   private IterationManager iterationManager;
   private RemoveCacheListener removeCacheListener;
   private ClientCounterManagerNotificationManager clientCounterNotificationManager;
   private HotRodAccessLogging accessLogging = new HotRodAccessLogging();
   private ScheduledExecutorService scheduledExecutor;

   public HotRodServer() {
      super("HotRod");
   }

   public boolean hasDefaultCache() {
      return hasDefaultCache;
   }

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
      return new HotRodDecoder(cacheManager, getExecutor(), this);
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

      static final byte[] bytes = Util.EMPTY_BYTE_ARRAY;

      @Override
      public Object filterAndConvert(Object key, Object value, Metadata metadata) {
         return bytes;
      }

      @Override
      public MediaType format() {
         return null;
      }

      public static final class ToEmptyBytesKeyValueFilterConverterExternalizer implements
                                                                                Externalizer<ToEmptyBytesKeyValueFilterConverter> {

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
      this.hasDefaultCache = cacheManager.getCacheManagerConfiguration().defaultCacheName().isPresent();

      // Initialize query-specific stuff
      List<QueryFacade> queryFacades = loadQueryFacades();
      queryFacade = queryFacades.size() > 0 ? queryFacades.get(0) : null;
      clientListenerRegistry = new ClientListenerRegistry(
         cacheManager.getGlobalComponentRegistry().getComponent(EncoderRegistry.class));
      clientCounterNotificationManager = new ClientCounterManagerNotificationManager(asCounterManager(cacheManager));

      addKeyValueFilterConverterFactory(ToEmptyBytesKeyValueFilterConverter.class.getName(), new ToEmptyBytesFactory());

      addCacheEventConverterFactory("key-value-with-previous-converter-factory",
                                    new KeyValueWithPreviousEventConverterFactory());
      addCacheEventConverterFactory("___eager-key-value-version-converter", KeyValueVersionConverterFactory.SINGLETON);
      loadFilterConverterFactories(ParamKeyValueFilterConverterFactory.class, this::addKeyValueFilterConverterFactory);
      loadFilterConverterFactories(CacheEventFilterConverterFactory.class, this::addCacheEventFilterConverterFactory);
      loadFilterConverterFactories(CacheEventConverterFactory.class, this::addCacheEventConverterFactory);
      loadFilterConverterFactories(KeyValueFilterConverterFactory.class, this::addKeyValueFilterConverterFactory);

      DefaultThreadFactory factory = new DefaultThreadFactory(getQualifiedName() + "-Scheduled");
      scheduledExecutor = Executors.newSingleThreadScheduledExecutor(factory);

      removeCacheListener = new RemoveCacheListener();
      SecurityActions.addListener(cacheManager, removeCacheListener);

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

   @Override
   public ChannelInitializer<Channel> getInitializer() {
      if (configuration.idleTimeout() > 0)
         return new NettyInitializers(new NettyChannelInitializer(this, transport, getEncoder(), getDecoder()),
                                      new TimeoutEnabledChannelInitializer<>(this));
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
      if (hasDefaultCache) {
         getCacheInfo("", (byte) 0, 0, false);
      }
   }

   private void preStartCaches() {
      // Start defined caches to avoid issues with lazily started caches
      // Skip internal caches
      for (String cacheName : cacheManager.getCacheNames()) {
         getCacheInfo(cacheName, (byte) 0, 0, false);
      }

      scheduledExecutor.scheduleWithFixedDelay(new CacheInfoUpdateTask(),
                                               LISTENERS_CHECK_INTERVAL, LISTENERS_CHECK_INTERVAL, TimeUnit.SECONDS);
   }

   private void addSelfToTopologyView(EmbeddedCacheManager cacheManager) {
      addressCache = cacheManager.getCache(configuration.topologyCacheName());
      clusterAddress = cacheManager.getAddress();
      address = new ServerAddress(configuration.publicHost(), configuration.publicPort());
      clusterExecutor = cacheManager.executor();

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
      InternalCacheRegistry internalCacheRegistry = cacheManager.getGlobalComponentRegistry().getComponent(
         InternalCacheRegistry.class);
      internalCacheRegistry.registerInternalCache(configuration.topologyCacheName(),
                                                  createTopologyCacheConfig(
                                                     cacheManager.getCacheManagerConfiguration().transport()
                                                                 .distributedSyncTimeout()).build(),
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

   public AdvancedCache<byte[], byte[]> cache(CacheInfo cacheInfo, HotRodHeader header, Subject subject) {
      KeyValuePair<MediaType, MediaType> requestMediaTypes = getRequestMediaTypes(header, cacheInfo.configuration);
      AdvancedCache<byte[], byte[]> cache =
         cacheInfo.getCache(requestMediaTypes, subject);
      cache = header.getOptimizedCache(cache, cacheInfo.transactional, cacheInfo.clustered);
      return cache;
   }

   public EmbeddedMultimapCache<WrappedByteArray, WrappedByteArray> multimap(HotRodHeader header, Subject subject) {
      return new EmbeddedMultimapCache(cache(getCacheInfo(header), header, subject));
   }

   boolean hasSyncListener(CacheNotifierImpl<?, ?> cacheNotifier) {
      for (Class<? extends Annotation> annotation :
            new Class[]{CacheEntryCreated.class, CacheEntryRemoved.class, CacheEntryExpired.class, CacheEntryModified.class}) {
         for (CacheEntryListenerInvocation invocation : cacheNotifier.getListenerCollectionForAnnotation(annotation)) {
            if (invocation.isSync()) {
               return true;
            }
         }
      }
      return false;
   }

   public CacheInfo getCacheInfo(HotRodHeader header) {
      return getCacheInfo(header.cacheName, header.version, header.messageId, true);
   }

   public CacheInfo getCacheInfo(String cacheName, byte hotRodVersion, long messageId, boolean checkIgnored) {
      if (checkIgnored && isCacheIgnored(cacheName)) {
         throw new CacheUnavailableException();
      }
      CacheInfo info = knownCaches.get(cacheName);
      if (info == null) {
         boolean keep = checkCacheIsAvailable(cacheName, hotRodVersion, messageId);

         AdvancedCache<byte[], byte[]> cache = obtainAnonymizedCache(cacheName);
         Configuration cacheCfg = SecurityActions.getCacheConfiguration(cache);
         info = new CacheInfo(cache, cacheCfg);
         updateCacheInfo(info);
         if (keep) {
            knownCaches.put(cacheName, info);
         }
      }
      return info;
   }

   private boolean checkCacheIsAvailable(String cacheName, byte hotRodVersion, long messageId) {
      InternalCacheRegistry icr = cacheManager.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      boolean keep;
      if (icr.isPrivateCache(cacheName)) {
         throw new RequestParsingException(
            String.format("Remote requests are not allowed to private caches. Do no send remote requests to cache '%s'",
                          cacheName),
            hotRodVersion, messageId);
      } else if (icr.internalCacheHasFlag(cacheName, InternalCacheRegistry.Flag.PROTECTED)) {
         // We want to make sure the cache access is checked every time, so don't store it as a "known" cache.
         // More expensive, but these caches should not be accessed frequently
         keep = false;
      } else if (!cacheName.isEmpty() && !cacheManager.getCacheNames().contains(cacheName)) {
         throw new CacheNotFoundException(
               String.format("Cache with name '%s' not found amongst the configured caches", cacheName),
               hotRodVersion, messageId);
      } else if (cacheName.isEmpty() && !hasDefaultCache) {
         throw new CacheNotFoundException("Default cache requested but not configured", hotRodVersion, messageId);
      } else {
         keep = true;
      }
      return keep;
   }

   public void updateCacheInfo(CacheInfo info) {
      if (info.anonymizedCache.getStatus() != ComponentStatus.RUNNING)
         return;

      ComponentRegistry cr = SecurityActions.getCacheComponentRegistry(info.anonymizedCache);
      PersistenceManager pm = cr.getComponent(PersistenceManager.class);
      boolean hasIndexing = SecurityActions.getCacheConfiguration(info.anonymizedCache).indexing().index().isEnabled();
      CacheNotifierImpl cacheNotifier = (CacheNotifierImpl) cr.getComponent(CacheNotifier.class);
      info.update(pm.isEnabled(), hasIndexing, hasSyncListener(cacheNotifier));
   }

   private AdvancedCache<byte[], byte[]> obtainAnonymizedCache(String cacheName) {
      String validCacheName = cacheName.isEmpty() ? defaultCacheName() : cacheName;
      Cache<byte[], byte[]> cache = SecurityActions.getCache(cacheManager, validCacheName);
      AdvancedCache<byte[], byte[]> advancedCache = cache.getAdvancedCache();
      // make sure we register a Migrator for this cache!
      tryRegisterMigrationManager(advancedCache);
      return advancedCache;
   }

   private void tryRegisterMigrationManager(AdvancedCache<byte[], byte[]> cache) {
      ComponentRegistry cr = SecurityActions.getCacheComponentRegistry(cache.getAdvancedCache());
      RollingUpgradeManager migrationManager = cr.getComponent(RollingUpgradeManager.class);
      if (migrationManager != null) migrationManager.addSourceMigrator(new HotRodSourceMigrator(cache));
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

   private static KeyValuePair<MediaType, MediaType> getRequestMediaTypes(HotRodHeader header,
                                                                          Configuration configuration) {
      MediaType keyRequestType = header == null ? APPLICATION_UNKNOWN : header.getKeyMediaType();
      MediaType valueRequestType = header == null ? APPLICATION_UNKNOWN : header.getValueMediaType();
      if (header != null && HotRodVersion.HOTROD_28.isOlder(header.version)) {
         // Pre-2.8 clients always send protobuf payload to the metadata cache
         if (header.cacheName.equals(PROTOBUF_METADATA_CACHE_NAME)) {
            keyRequestType = APPLICATION_PROTOSTREAM;
            valueRequestType = APPLICATION_PROTOSTREAM;
         } else {
            // Pre-2.8 clients always sent query encoded as protobuf unless object store is used.
            if (header.op == HotRodOperation.QUERY) {
               boolean objectStorage = APPLICATION_OBJECT.match(configuration.encoding().valueDataType().mediaType());
               keyRequestType = objectStorage ? APPLICATION_JBOSS_MARSHALLING : APPLICATION_PROTOSTREAM;
               valueRequestType = objectStorage ? APPLICATION_JBOSS_MARSHALLING : APPLICATION_PROTOSTREAM;
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
      if (cacheManager != null && Configurations.isClustered(cacheManager.getCacheManagerConfiguration())) {
         InternalCacheRegistry internalCacheRegistry = cacheManager.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
         if (internalCacheRegistry != null)
            internalCacheRegistry.unregisterInternalCache(configuration.topologyCacheName());
      }
      if (scheduledExecutor != null) {
         scheduledExecutor.shutdownNow();
      }

      if (clientListenerRegistry != null) clientListenerRegistry.stop();
      if (clientCounterNotificationManager != null) clientCounterNotificationManager.stop();
      super.stop();
   }

   public HotRodAccessLogging accessLogging() {
      return accessLogging;
   }

   public Metadata.Builder buildMetadata(long lifespan, TimeUnitValue lifespanUnit, long maxIdle, TimeUnitValue maxIdleUnit) {
      EmbeddedMetadata.Builder metadata = new EmbeddedMetadata.Builder();
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
      return metadata;
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

   public String toString() {
      return "HotRodServer[" +
            "configuration=" + configuration +
            ']';
   }

   public static class CacheInfo {
      final AdvancedCache<byte[], byte[]> anonymizedCache;
      final Map<KeyValuePair<MediaType, MediaType>, AdvancedCache<byte[], byte[]>> encodedCaches =
         new ConcurrentHashMap<>();
      final DistributionManager distributionManager;
      final VersionGenerator versionGenerator;
      final Configuration configuration;
      final boolean transactional;
      final boolean clustered;
      volatile boolean persistence;
      volatile boolean indexing;
      volatile boolean syncListener;

      CacheInfo(AdvancedCache<byte[], byte[]> cache, Configuration configuration) {
         this.anonymizedCache = SecurityActions.anonymizeSecureCache(cache);
         this.distributionManager = SecurityActions.getDistributionManager(cache);
         ComponentRegistry componentRegistry = SecurityActions.getCacheComponentRegistry(cache);
         this.versionGenerator = componentRegistry.getVersionGenerator();
         this.configuration = configuration;
         this.transactional = configuration.transaction().transactionMode().isTransactional();
         this.clustered = configuration.clustering().cacheMode().isClustered();

         // Start conservative and assume we have all the stuff that can cause operations to block
         this.persistence = true;
         this.indexing = true;
         this.syncListener = true;
      }

      AdvancedCache<byte[], byte[]> getCache(KeyValuePair<MediaType, MediaType> requestMediaTypes, Subject subject) {
         AdvancedCache<byte[], byte[]> cache = encodedCaches.get(requestMediaTypes);
         if (cache == null) {
            // The client always sends byte[] keys and values
            cache = (AdvancedCache<byte[], byte[]>) anonymizedCache.withMediaType(
               requestMediaTypes.getKey().getTypeSubtype(), requestMediaTypes.getValue().getTypeSubtype());
            encodedCaches.put(requestMediaTypes, cache);
         }
         if (subject == null) {
            return cache;
         } else {
            return cache.withSubject(subject);
         }
      }

      public void update(boolean enabled, boolean indexing, boolean syncListener) {
         this.persistence = enabled;
         this.indexing = indexing;
         this.syncListener = syncListener;
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
         while (!success && addressCache.getStatus().allowInvocations()) {
            try {
               // No need for a timeout here, the cluster executor has a default timeout
               CompletableFuture<Void> future = clusterExecutor.submitConsumer(new CheckAddressTask(addressCache.getName(),
                     clusterAddress), (a, v, t) -> {
                  if (t != null) {
                     throw new CacheException(t);
                  }
                  if (!v) {
                     log.debugf("Re-adding %s to the topology cache", clusterAddress);
                     addressCache.putAsync(clusterAddress, address);
                  }
               });
               future.get();
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
         knownCaches.remove(event.getCacheName());
      }
   }

   private class CacheInfoUpdateTask implements Runnable {
      @Override
      public void run() {
         for (CacheInfo cacheInfo : knownCaches.values()) {
            updateCacheInfo(cacheInfo);
         }
      }
   }
}

class CheckAddressTask implements Function<EmbeddedCacheManager, Boolean>, Serializable {
   private final String cacheName;
   private final Address clusterAddress;

   CheckAddressTask(String cacheName, Address clusterAddress) {
      this.cacheName = cacheName;
      this.clusterAddress = clusterAddress;
   }

   @Override
   public Boolean apply(EmbeddedCacheManager embeddedCacheManager) {
      if (embeddedCacheManager.isRunning(cacheName)) {
         Cache<Address, ServerAddress> cache = embeddedCacheManager.getCache(cacheName);
         return cache.containsKey(clusterAddress);
      }
      // If the cache isn't started just play like this node has the address in the cache - it will be added as it
      // joins, so no worries
      return true;
   }
}
