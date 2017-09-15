package org.infinispan.server.hotrod;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import javax.security.sasl.SaslServerFactory;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.commons.dataconversion.CompatModeEncoder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.context.Flag;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.filter.NamedFactory;
import org.infinispan.filter.ParamKeyValueFilterConverterFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.query.remote.ProtostreamCompatEncoder;
import org.infinispan.query.remote.client.BaseProtoStreamMarshaller;
import org.infinispan.query.remote.impl.ProtostreamWrapper;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.QueryFacade;
import org.infinispan.server.core.security.SaslUtils;
import org.infinispan.server.core.transport.NettyInitializers;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.hotrod.event.KeyValueWithPreviousEventConverterFactory;
import org.infinispan.server.hotrod.iteration.DefaultIterationManager;
import org.infinispan.server.hotrod.iteration.IterationManager;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.transport.HotRodChannelInitializer;
import org.infinispan.server.hotrod.transport.TimeoutEnabledChannelInitializer;
import org.infinispan.upgrade.RollingUpgradeManager;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Hot Rod server, in charge of defining its encoder/decoder and, if clustered, update the topology information on
 * startup and shutdown.
 * <p>
 * TODO: It's too late for 5.1.1 series. In 5.2, split class into: local and cluster hot rod servers This should safe
 * some memory for the local case and the code should be cleaner
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public class HotRodServer extends AbstractProtocolServer<HotRodServerConfiguration> {

   private static final Log log = LogFactory.getLog(HotRodServer.class, Log.class);

   private static final String WORKER_THREADS_SYS_PROP = "infinispan.server.hotrod.workerThreads";

   public HotRodServer() {
      super("HotRod");
   }

   private boolean isClustered;
   private Address clusterAddress;
   private ServerAddress address;
   private Cache<Address, ServerAddress> addressCache;
   private Map<String, AdvancedCache> knownCaches = CollectionFactory.makeConcurrentMap(4, 0.9f, 16);
   private Map<String, Configuration> knownCacheConfigurations = CollectionFactory.makeConcurrentMap(4, 0.9f, 16);
   private Map<String, ComponentRegistry> knownCacheRegistries = CollectionFactory.makeConcurrentMap(4, 0.9f, 16);
   private QueryFacade queryFacade;
   private Map<String, SaslServerFactory> saslMechFactories = CollectionFactory.makeConcurrentMap(4, 0.9f, 16);
   private ClientListenerRegistry clientListenerRegistry;
   private Marshaller marshaller;
   private DefaultExecutorService distributedExecutorService;
   private CrashedMemberDetectorListener viewChangeListener;
   private ReAddMyAddressListener topologyChangeListener;
   protected ExecutorService executor;
   private IterationManager iterationManager;
   private RemoveCacheListener removeCacheListener;

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

   @Override
   public ChannelOutboundHandler getEncoder() {
      return new HotRodEncoder(cacheManager, this);
   }

   @Override
   public HotRodDecoder getDecoder() {
      return new HotRodDecoder(cacheManager, transport, this, this::isCacheIgnored);
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
   @SerializeWith(value=ToEmptyBytesKeyValueFilterConverter.ToEmptyBytesKeyValueFilterConverterExternalizer.class)
   static class ToEmptyBytesKeyValueFilterConverter extends AbstractKeyValueFilterConverter {
      private ToEmptyBytesKeyValueFilterConverter() { }

      public static ToEmptyBytesKeyValueFilterConverter INSTANCE = new ToEmptyBytesKeyValueFilterConverter();

      static final byte[] bytes = new byte[0];

      @Override
      public Object filterAndConvert(Object key, Object value, Metadata metadata) {
         return bytes;
      }

      public static final class ToEmptyBytesKeyValueFilterConverterExternalizer implements Externalizer<ToEmptyBytesKeyValueFilterConverter> {

         @Override
         public void writeObject(ObjectOutput output, ToEmptyBytesKeyValueFilterConverter object) throws IOException { }

         @Override
         public ToEmptyBytesKeyValueFilterConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return INSTANCE;
         }
      }
   }

   @Override
   protected void startInternal(HotRodServerConfiguration configuration, EmbeddedCacheManager cacheManager) {
      // These are also initialized by super.startInternal, but we need them before
      this.configuration = configuration;
      this.cacheManager = cacheManager;
      this.iterationManager = new DefaultIterationManager(cacheManager);

      // populate the sasl factories based on the required mechs
      setupSasl();

      // Initialize query-specific stuff
      List<QueryFacade> queryFacades = loadQueryFacades();
      queryFacade = queryFacades.size() > 0 ? queryFacades.get(0) : null;
      clientListenerRegistry = new ClientListenerRegistry(configuration);

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

   AbortPolicy abortPolicy = new AbortPolicy() {
      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
         if (executor.isShutdown())
            throw new IllegalLifecycleStateException("Server has been stopped");
         else
            super.rejectedExecution(r, e);
      }
   };

   public ExecutorService getExecutor(String threadPrefix) {
      if (this.executor == null || this.executor.isShutdown()) {
         DefaultThreadFactory factory = new DefaultThreadFactory(threadPrefix + "-ServerHandler");
         int workerThreads = Integer.getInteger(WORKER_THREADS_SYS_PROP, configuration.workerThreads());
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
         return new NettyInitializers(Arrays.asList(new HotRodChannelInitializer(this, transport, getEncoder(), getDecoder(),
               getExecutor(getQualifiedName())), new TimeoutEnabledChannelInitializer<>(this)));
      else // Idle timeout logic is disabled with -1 or 0 values
         return new NettyInitializers(new HotRodChannelInitializer(this, transport, getEncoder(), getDecoder(),
               getExecutor(getQualifiedName())));
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
      getCacheInstance(configuration.defaultCacheName(), cacheManager, true, true);
   }

   private void preStartCaches() {
      // Start defined caches to avoid issues with lazily started caches. Skip internal caches if authorization is not
      // enabled
      InternalCacheRegistry icr = cacheManager.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      boolean authz = cacheManager.getCacheManagerConfiguration().security().authorization().enabled();
      for (String cacheName : cacheManager.getCacheNames()) {
         getCacheInstance(cacheName, cacheManager, false, (!icr.internalCacheHasFlag(cacheName, InternalCacheRegistry.Flag.PROTECTED) || authz));
      }
   }

   private void addSelfToTopologyView(EmbeddedCacheManager cacheManager) {
      addressCache = cacheManager.getCache(configuration.topologyCacheName());
      clusterAddress = cacheManager.getAddress();
      address = new ServerAddress(configuration.proxyHost(), configuration.proxyPort());
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

   AdvancedCache getKnownCache(String cacheName) {
      return knownCaches.get(cacheName);
   }

   AdvancedCache getCacheInstance(String cacheName, EmbeddedCacheManager cacheManager, Boolean skipCacheCheck, Boolean addToKnownCaches) {
      AdvancedCache cache = null;
      if (!skipCacheCheck) cache = knownCaches.get(cacheName);

      if (cache == null) {
         String validCacheName = cacheName.isEmpty() ? configuration.defaultCacheName() : cacheName;
         Cache<byte[], byte[]> tmpCache = SecurityActions.getCache(cacheManager, validCacheName);
         Configuration cacheConfiguration = SecurityActions.getCacheConfiguration(tmpCache.getAdvancedCache());
         boolean compatibility = cacheConfiguration.compatibility().enabled();
         Marshaller marshaller = cacheConfiguration.compatibility().marshaller();
         boolean indexing = cacheConfiguration.indexing().index().isEnabled();
         ComponentRegistry cacheComponentRegistry = SecurityActions.getCacheComponentRegistry(tmpCache.getAdvancedCache());
         if (marshaller != null) {
            cacheComponentRegistry.wireDependencies(marshaller);
         }

         // Use flag when compatibility is enabled, otherwise it's unnecessary
         if (compatibility || indexing) {
            cache = tmpCache.getAdvancedCache().withFlags(Flag.OPERATION_HOTROD);
            if (compatibility && !indexing) {
               cache = cache.getAdvancedCache().withEncoding(CompatModeEncoder.class);
            }
            if (compatibility && indexing) {
               if (marshaller instanceof BaseProtoStreamMarshaller) {
                  cache = cache.getAdvancedCache().withEncoding(ProtostreamCompatEncoder.class).withWrapping(ByteArrayWrapper.class, ProtostreamWrapper.class);
               } else {
                  cache = cache.getAdvancedCache().withEncoding(CompatModeEncoder.class);
               }
            }
            if (!compatibility) {
               cache = cache.getAdvancedCache().withWrapping(ByteArrayWrapper.class, ProtostreamWrapper.class);
            }
         } else
            cache = tmpCache.getAdvancedCache();

         // We don't need synchronization as long as we store the cache last
         knownCacheConfigurations.put(cacheName, cacheConfiguration);
         knownCacheRegistries.put(cacheName, SecurityActions.getCacheComponentRegistry(tmpCache.getAdvancedCache()));
         if (addToKnownCaches) {
            knownCaches.put(cacheName, cache);
         }
         // make sure we register a Migrator for this cache!
         tryRegisterMigrationManager(cacheName, cache);
      }

      return cache;
   }

   Configuration getCacheConfiguration(String cacheName) {
      return knownCacheConfigurations.get(cacheName);
   }

   ComponentRegistry getCacheRegistry(String cacheName) {
      return knownCacheRegistries.get(cacheName);
   }

   void tryRegisterMigrationManager(String cacheName, AdvancedCache<byte[], byte[]> cache) {
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

   private Cache<Address, ServerAddress> getAddressCache() {
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
      iterationManager.setMarshaller(optMarshaller);
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
      if (executor != null) executor.shutdownNow();
      super.stop();
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
         knownCaches.remove(event.getCacheName());
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
