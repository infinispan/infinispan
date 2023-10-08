package org.infinispan.server.hotrod;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JBOSS_MARSHALLING;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.counter.EmbeddedCounterManagerFactory.asCounterManager;
import static org.infinispan.factories.KnownComponentNames.NON_BLOCKING_EXECUTOR;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import javax.security.auth.Subject;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.internal.InternalCacheNames;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.impl.BasicComponentRegistry;
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
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.annotation.ConfigurationChanged;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ConfigurationChangedEvent;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.CacheInfo;
import org.infinispan.server.core.QueryFacade;
import org.infinispan.server.core.ServerConstants;
import org.infinispan.server.core.transport.FlushConsolidationInitializer;
import org.infinispan.server.core.transport.NettyChannelInitializer;
import org.infinispan.server.core.transport.NettyInitializers;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.hotrod.counter.listener.ClientCounterManagerNotificationManager;
import org.infinispan.server.hotrod.event.KeyValueWithPreviousEventConverterFactory;
import org.infinispan.server.hotrod.logging.HotRodAccessLogging;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.streaming.StreamingManager;
import org.infinispan.server.hotrod.transport.TimeoutEnabledChannelInitializer;
import org.infinispan.server.iteration.DefaultIterationManager;
import org.infinispan.server.iteration.IterationManager;
import org.infinispan.util.KeyValuePair;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.group.ChannelMatcher;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Hot Rod server, in charge of defining its encoder/decoder and, if clustered, update the topology information on
 * startup and shutdown.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public class HotRodServer extends AbstractProtocolServer<HotRodServerConfiguration> {
   static final Log log = LogFactory.getLog(HotRodServer.class, Log.class);

   private static final long MILLISECONDS_IN_30_DAYS = TimeUnit.DAYS.toMillis(30);

   public static final int DEFAULT_HOTROD_PORT = 11222;
   public static final int LISTENERS_CHECK_INTERVAL = 10;

   private boolean hasDefaultCache;
   private ServerAddress address;
   private Cache<Address, ServerAddress> addressCache;
   private final Map<String, ExtendedCacheInfo> knownCaches = new ConcurrentHashMap<>();
   private QueryFacade queryFacade;
   private ClientListenerRegistry clientListenerRegistry;
   private Marshaller marshaller;
   private ClusterExecutor clusterExecutor;
   private CrashedMemberDetectorListener viewChangeListener;
   private ReAddMyAddressListener topologyChangeListener;
   private IterationManager iterationManager;
   private StreamingManager streamingManager;
   private ServerCacheListener serverCacheListener;
   private ClientCounterManagerNotificationManager clientCounterNotificationManager;
   private final HotRodAccessLogging accessLogging = new HotRodAccessLogging();
   private ScheduledExecutorService scheduledExecutor;
   private TimeService timeService;
   private InternalCacheRegistry internalCacheRegistry;
   private ConfigurationManager configurationManager;

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

   public TimeService getTimeService() {
      return timeService;
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

   @Override
   public ChannelMatcher getChannelMatcher() {
      return channel -> channel.pipeline().get(HotRodDecoder.class) != null;
   }

   @Override
   public void installDetector(Channel ch) {
      ch.pipeline().addLast(HotRodDetector.NAME, new HotRodDetector(this));
   }

   /**
    * Class used to create to empty filter converters that ignores marshalling of keys and values
    */
   static class ToEmptyBytesFactory implements ParamKeyValueFilterConverterFactory {
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
   @ProtoTypeId(ProtoStreamTypeIds.SERVER_HR_TO_EMPTY_BYTES_KEY_VALUE_FILTER_CONVERTER)
   static class ToEmptyBytesKeyValueFilterConverter extends AbstractKeyValueFilterConverter {
      static ToEmptyBytesKeyValueFilterConverter INSTANCE = new ToEmptyBytesKeyValueFilterConverter();

      static final byte[] bytes = Util.EMPTY_BYTE_ARRAY;

      @ProtoFactory
      static ToEmptyBytesKeyValueFilterConverter protoFactory() {
         return INSTANCE;
      }

      @Override
      public Object filterAndConvert(Object key, Object value, Metadata metadata) {
         return bytes;
      }

      @Override
      public MediaType format() {
         return null;
      }
   }

   @Override
   protected void startInternal() {
      GlobalComponentRegistry gcr = SecurityActions.getGlobalComponentRegistry(cacheManager);
      internalCacheRegistry = gcr.getComponent(InternalCacheRegistry.class);
      configurationManager = gcr.getComponent(ConfigurationManager.class);
      this.iterationManager = new DefaultIterationManager(gcr.getTimeService());
      this.streamingManager = new StreamingManager(gcr.getTimeService());
      this.hasDefaultCache = configuration.defaultCacheName() != null || cacheManager.getCacheManagerConfiguration().defaultCacheName().isPresent();

      // Initialize query-specific stuff
      queryFacade = loadQueryFacade();
      clientListenerRegistry = new ClientListenerRegistry(gcr.getComponent(EncoderRegistry.class),
            gcr.getComponent(ExecutorService.class, NON_BLOCKING_EXECUTOR));
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

      serverCacheListener = new ServerCacheListener();
      SecurityActions.addListener(cacheManager, serverCacheListener);

      // Start default cache and the endpoint before adding self to
      // topology in order to avoid topology updates being used before
      // endpoint is available.
      super.startInternal();
   }

   @Override
   public void internalPostStart() {
      super.internalPostStart();

      // Add self to topology cache last, after everything is initialized
      if (Configurations.isClustered(SecurityActions.getCacheManagerConfiguration(cacheManager))) {
         defineTopologyCacheConfig(cacheManager);
         if (log.isDebugEnabled())
            log.debugf("Externally facing address is %s:%d", configuration.proxyHost(), configuration.proxyPort());

         addSelfToTopologyView(cacheManager);
      }
   }

   @Override
   public ChannelInitializer<Channel> getInitializer() {
      if (configuration.idleTimeout() > 0)
         return new NettyInitializers(new NettyChannelInitializer(this, transport, getEncoder(), this::getDecoder),
                                      new FlushConsolidationInitializer(),
                                      new TimeoutEnabledChannelInitializer<>(this));
      else // Idle timeout logic is disabled with -1 or 0 values
         return new NettyInitializers(new NettyChannelInitializer(this, transport, getEncoder(), this::getDecoder),
               new FlushConsolidationInitializer());
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

   private QueryFacade loadQueryFacade() {
      QueryFacade facadeImpl = null;
      Iterator<QueryFacade> iterator = ServiceLoader.load(QueryFacade.class, getClass().getClassLoader()).iterator();
      if (iterator.hasNext()) {
         facadeImpl = iterator.next();
         if (iterator.hasNext()) {
            throw new IllegalStateException("Found multiple QueryFacade service implementations: "
                                                  + facadeImpl.getClass().getName() + " and "
                                                  + iterator.next().getClass().getName());
         }
      }
      return facadeImpl;
   }

   @Override
   protected void startTransport() {
      super.startTransport();
   }

   @Override
   protected void startCaches() {
      super.startCaches();

      // Periodically update cache info about potentially blocking operations
      scheduledExecutor.scheduleWithFixedDelay(new CacheInfoUpdateTask(),
                                               LISTENERS_CHECK_INTERVAL, LISTENERS_CHECK_INTERVAL, TimeUnit.SECONDS);
   }

   private void addSelfToTopologyView(EmbeddedCacheManager cacheManager) {
      addressCache = cacheManager.getCache(configuration.topologyCacheName());
      Address clusterAddress = cacheManager.getAddress();
      address = ServerAddress.forAddress(configuration.publicHost(), configuration.publicPort(), configuration.networkPrefixOverride());
      clusterExecutor = cacheManager.executor();

      viewChangeListener = new CrashedMemberDetectorListener(addressCache, this);
      cacheManager.addListener(viewChangeListener);
      topologyChangeListener = new ReAddMyAddressListener(addressCache, clusterAddress, address);
      addressCache.addListener(topologyChangeListener);

      timeService = SecurityActions.getGlobalComponentRegistry(cacheManager).getTimeService();

      // Map cluster address to server endpoint address
      log.debugf("Map %s cluster address with %s server endpoint in address cache", clusterAddress, address);
      addressCache.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES)
            .put(clusterAddress, address);
   }

   private void defineTopologyCacheConfig(EmbeddedCacheManager cacheManager) {
      InternalCacheRegistry internalCacheRegistry = SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(
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
         builder.persistence()
               .addClusterLoader()
                  .segmented(false)
               .remoteCallTimeout(configuration.topologyReplTimeout());
      }

      return builder;
   }

   public AdvancedCache<byte[], byte[]> cache(ExtendedCacheInfo cacheInfo, HotRodHeader header, Subject subject) {
      KeyValuePair<MediaType, MediaType> requestMediaTypes = getRequestMediaTypes(header, cacheInfo.configuration);
      AdvancedCache<byte[], byte[]> cache =
         cacheInfo.getCache(requestMediaTypes, subject);
      cache = header.getOptimizedCache(cache, cacheInfo.transactional, cacheInfo.clustered);
      return cache;
   }

   public EmbeddedMultimapCache<byte[], byte[]> multimap(HotRodHeader header, Subject subject, boolean supportsDuplicates) {
      AdvancedCache<byte[], byte[]> cache = cache(getCacheInfo(header), header, subject).withStorageMediaType();
      return new EmbeddedMultimapCache(cache, supportsDuplicates);
   }

   public ExtendedCacheInfo getCacheInfo(HotRodHeader header) {
      return getCacheInfo(header.cacheName, header.version, header.messageId, true);
   }

   public ExtendedCacheInfo getCacheInfo(String cacheName, byte hotRodVersion, long messageId, boolean checkIgnored) {
      if (checkIgnored && isCacheIgnored(cacheName)) {
         throw new CacheUnavailableException();
      }
      ExtendedCacheInfo info = knownCaches.get(cacheName);
      if (info == null) {
         boolean keep = checkCacheIsAvailable(cacheName, hotRodVersion, messageId);

         AdvancedCache<byte[], byte[]> cache = obtainAnonymizedCache(cacheName);
         Configuration cacheCfg = SecurityActions.getCacheConfiguration(cache);
         info = new ExtendedCacheInfo(cache, cacheCfg);
         updateCacheInfo(info);
         if (keep) {
            knownCaches.put(cacheName, info);
         }
      }
      return info;
   }

   private boolean checkCacheIsAvailable(String cacheName, byte hotRodVersion, long messageId) {
      if (internalCacheRegistry.isPrivateCache(cacheName)) {
         throw new RequestParsingException(
            String.format("Remote requests are not allowed to private caches. Do no send remote requests to cache '%s'",
                          cacheName),
            hotRodVersion, messageId);
      } else if (internalCacheRegistry.internalCacheHasFlag(cacheName, InternalCacheRegistry.Flag.PROTECTED)) {
         // We want to make sure the cache access is checked every time, so don't store it as a "known" cache.
         // More expensive, but these caches should not be accessed frequently
         return false;
      } else if (!cacheName.isEmpty() && !cacheManager.getCacheNames().contains(cacheName) && !configurationManager.getAliases().contains(cacheName)) {
         throw new CacheNotFoundException(
               String.format("Cache with name '%s' not found amongst the configured caches", cacheName),
               hotRodVersion, messageId);
      } else if (cacheName.isEmpty() && !hasDefaultCache) {
         throw new CacheNotFoundException("Default cache requested but not configured", hotRodVersion, messageId);
      } else {
         return true;
      }
   }

   public void updateCacheInfo(ExtendedCacheInfo info) {
      if (info.getCache().getStatus() != ComponentStatus.RUNNING)
         return;

      boolean hasIndexing = SecurityActions.getCacheConfiguration(info.getCache()).indexing().enabled();
      info.update(hasIndexing);
   }

   private AdvancedCache<byte[], byte[]> obtainAnonymizedCache(String cacheName) {
      String validCacheName = cacheName.isEmpty() ? defaultCacheName() : cacheName;
      Cache<byte[], byte[]> cache = SecurityActions.getCache(cacheManager, validCacheName);
      return cache.getAdvancedCache();
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

   public StreamingManager getStreamingManager() {
      return streamingManager;
   }

   private static KeyValuePair<MediaType, MediaType> getRequestMediaTypes(HotRodHeader header,
                                                                          Configuration configuration) {
      MediaType keyRequestType = header == null ? APPLICATION_UNKNOWN : header.getKeyMediaType();
      MediaType valueRequestType = header == null ? APPLICATION_UNKNOWN : header.getValueMediaType();
      if (header != null && HotRodVersion.HOTROD_28.isOlder(header.version)) {
         // Pre-2.8 clients always send protobuf payload to the metadata cache
         if (header.cacheName.equals(InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME)) {
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
      if (log.isDebugEnabled())
         log.debugf("Stopping server %s listening at %s:%d", getQualifiedName(), configuration.host(), configuration.port());

      AggregateCompletionStage<Void> removeAllStage = CompletionStages.aggregateCompletionStage();
      if (serverCacheListener != null) {
         removeAllStage.dependsOn(SecurityActions.removeListenerAsync(cacheManager, serverCacheListener));
      }
      if (viewChangeListener != null) {
         removeAllStage.dependsOn(SecurityActions.removeListenerAsync(cacheManager, viewChangeListener));
      }
      if (topologyChangeListener != null) {
         removeAllStage.dependsOn(SecurityActions.removeListenerAsync(addressCache, topologyChangeListener));
      }
      CompletionStages.join(removeAllStage.freeze());
      if (cacheManager != null && Configurations.isClustered(SecurityActions.getCacheManagerConfiguration(cacheManager))) {
         InternalCacheRegistry internalCacheRegistry =
            SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(InternalCacheRegistry.class);
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

   public Metadata.Builder buildMetadata2x(long lifespan, TimeUnitValue lifespanUnit, long maxIdle, TimeUnitValue maxIdleUnit) {
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

   public Metadata.Builder buildMetadata(long lifespan, TimeUnitValue lifespanUnit, long maxIdle, TimeUnitValue maxIdleUnit) {
      EmbeddedMetadata.Builder metadata = new EmbeddedMetadata.Builder();
      if (lifespan != ServerConstants.EXPIRATION_DEFAULT && lifespanUnit != TimeUnitValue.DEFAULT) {
         if (lifespanUnit == TimeUnitValue.INFINITE) {
            metadata.lifespan(ServerConstants.EXPIRATION_NONE);
         } else {
            metadata.lifespan(lifespanUnit.toTimeUnit().toMillis(lifespan));
         }
      }
      if (maxIdle != ServerConstants.EXPIRATION_DEFAULT && maxIdleUnit != TimeUnitValue.DEFAULT) {
         if (maxIdleUnit == TimeUnitValue.INFINITE) {
            metadata.maxIdle(ServerConstants.EXPIRATION_NONE);
         } else {
            metadata.maxIdle(maxIdleUnit.toTimeUnit().toMillis(maxIdle));
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

   public String toString() {
      return "HotRodServer[" +
            "configuration=" + configuration +
            ']';
   }

   @SuppressWarnings("removal")
   private static VersionGenerator getHotRodVersionGenerator(AdvancedCache<?,?> cache) {
      return SecurityActions.getCacheComponentRegistry(cache)
            .getComponent(BasicComponentRegistry.class)
            .getComponent(KnownComponentNames.HOT_ROD_VERSION_GENERATOR, VersionGenerator.class)
            .running();
   }

   public static class ExtendedCacheInfo extends CacheInfo<byte[], byte[]> {
      final DistributionManager distributionManager;
      final VersionGenerator versionGenerator;
      final Configuration configuration;
      final boolean transactional;
      final boolean clustered;
      volatile boolean indexing;

      ExtendedCacheInfo(AdvancedCache<byte[], byte[]> cache, Configuration configuration) {
         super(SecurityActions.anonymizeSecureCache(cache));
         this.distributionManager = SecurityActions.getDistributionManager(cache);
         //Note: HotRod cannot use the same version generator as Optimistic Transaction.
         this.versionGenerator = getHotRodVersionGenerator(cache);
         this.configuration = configuration;
         this.transactional = configuration.transaction().transactionMode().isTransactional();
         this.clustered = configuration.clustering().cacheMode().isClustered();

         // Start conservative and assume we have all the stuff that can cause operations to block
         this.indexing = true;
      }

      public void update(boolean indexing) {
         this.indexing = indexing;
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
         recursionTopologyChanged();
      }

      private void recursionTopologyChanged() {
         // Check the manager status, not the address cache status, because it changes to STOPPING first
         if (cacheManager.getStatus().allowInvocations()) {
            // No need for a timeout here, the cluster executor has a default timeout
            clusterExecutor.submitConsumer(new CheckAddressTask(addressCache.getName(), clusterAddress), (a, v, t) -> {
               if (t != null && !(t instanceof IllegalLifecycleStateException)) {
                  log.debug("Error re-adding address to topology cache, retrying", t);
                  recursionTopologyChanged();
               }
               if (t == null && !v) {
                  log.debugf("Re-adding %s to the topology cache", clusterAddress);
                  addressCache.putAsync(clusterAddress, address);
               }
            });
         }
      }
   }

   @Listener
   class ServerCacheListener {
      @CacheStopped
      public void cacheStopped(CacheStoppedEvent event) {
         knownCaches.remove(event.getCacheName());
      }

      @ConfigurationChanged
      public void configurationChanged(ConfigurationChangedEvent event) {
         knownCaches.clear(); // TODO: be less aggressive
      }
   }

   private class CacheInfoUpdateTask implements Runnable {
      @Override
      public void run() {
         for (ExtendedCacheInfo cacheInfo : knownCaches.values()) {
            updateCacheInfo(cacheInfo);
         }
      }
   }
}
