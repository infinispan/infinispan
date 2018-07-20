package org.infinispan.factories;

import static java.util.Objects.requireNonNull;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingAdvancedCache;
import org.infinispan.cache.impl.CacheImpl;
import org.infinispan.cache.impl.EncoderCache;
import org.infinispan.cache.impl.SimpleCacheImpl;
import org.infinispan.cache.impl.StatsCollectingCache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.TranscoderMarshallerAdapter;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ContentTypeConfiguration;
import org.infinispan.configuration.cache.JMXStatisticsConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.ImmutableContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.eviction.impl.ActivationManagerStub;
import org.infinispan.eviction.impl.PassivationManagerStub;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.interceptors.impl.CacheMgmtInterceptor;
import org.infinispan.jmx.CacheJmxRegistration;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventManager;
import org.infinispan.notifications.cachelistener.cluster.impl.ClusterEventManagerStub;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerStub;
import org.infinispan.transaction.xa.recovery.RecoveryAdminOperations;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteAdminOperations;

/**
 * An internal factory for constructing Caches.  Used by the {@link org.infinispan.manager.DefaultCacheManager}, this is
 * not intended as public API.
 * <p/>
 * This is a special instance of a {@link AbstractComponentFactory} which contains bootstrap information for the {@link
 * ComponentRegistry}.
 * <p/>
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @since 4.0
 */
public class InternalCacheFactory<K, V> extends AbstractNamedCacheComponentFactory {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   /**
    * This implementation clones the configuration passed in before using it.
    *
    * @param configuration           to use
    * @param globalComponentRegistry global component registry to attach the cache to
    * @param cacheName               name of the cache
    * @return a cache
    * @throws CacheConfigurationException if there are problems with the cfg
    */
   public Cache<K, V> createCache(Configuration configuration, GlobalComponentRegistry globalComponentRegistry,
                                  String cacheName) throws CacheConfigurationException {
      try {
         if (configuration.compatibility().enabled()) {
            log.warnCompatibilityDeprecated(cacheName);
         }
         if (configuration.simpleCache()) {
            return createSimpleCache(configuration, globalComponentRegistry, cacheName);
         } else {
            return createAndWire(configuration, globalComponentRegistry, cacheName);
         }
      } catch (RuntimeException re) {
         throw re;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private AdvancedCache<K, V> createAndWire(Configuration configuration, GlobalComponentRegistry globalComponentRegistry,
                                             String cacheName) throws Exception {
      StreamingMarshaller marshaller = globalComponentRegistry.getOrCreateComponent(StreamingMarshaller.class, KnownComponentNames.INTERNAL_MARSHALLER);

      final BiFunction<DataConversion, DataConversion, AdvancedCache<K, V>> actualBuilder = (kc, kv) -> new CacheImpl<>(cacheName);
      BiFunction<DataConversion, DataConversion, AdvancedCache<K, V>> usedBuilder;
      // We can optimize REPL reads that meet some criteria. This allows us to bypass interceptor chain
      if (configuration.clustering().cacheMode().isReplicated() && !configuration.persistence().usingStores()
            && !configuration.transaction().transactionMode().isTransactional() && configuration.clustering().stateTransfer().awaitInitialTransfer()) {
         usedBuilder = (kc, kv) -> {
            AbstractGetAdvancedCache<K, V, ?> cache = new GetReplCache<>(actualBuilder.apply(kc, kv));
            if (configuration.jmxStatistics().available()) {
               cache = new StatsCache<>(cache);
            }
            if (configuration.clustering().partitionHandling().whenSplit() != PartitionHandling.ALLOW_READ_WRITES) {
               cache = new PartitionHandlingCache<>(cache);
            }
            return cache;
         };
      } else {
         usedBuilder = actualBuilder;
      }

      AdvancedCache<K, V> cache = buildEncodingCache(usedBuilder, configuration);

      bootstrap(cacheName, cache, configuration, globalComponentRegistry, marshaller);
      if (marshaller != null) {
         componentRegistry.wireDependencies(marshaller);
      }
      return cache;
   }

   private AdvancedCache<K, V> buildEncodingCache(BiFunction<DataConversion, DataConversion, AdvancedCache<K, V>> wrappedCacheBuilder, Configuration configuration) {
      ContentTypeConfiguration keyEncodingConfig = configuration.encoding().keyDataType();
      ContentTypeConfiguration valueEncodingConfig = configuration.encoding().valueDataType();

      MediaType keyType = keyEncodingConfig.mediaType();
      MediaType valueType = valueEncodingConfig.mediaType();

      DataConversion keyDataConversion = DataConversion.newKeyDataConversion(null, ByteArrayWrapper.class, keyType);
      DataConversion valueDataConversion = DataConversion.newValueDataConversion(null, ByteArrayWrapper.class, valueType);

      return new EncoderCache<>(wrappedCacheBuilder.apply(keyDataConversion, valueDataConversion), keyDataConversion, valueDataConversion);
   }

   private AdvancedCache<K, V> createSimpleCache(Configuration configuration, GlobalComponentRegistry globalComponentRegistry,
                                                 String cacheName) {
      AdvancedCache<K, V> cache;

      JMXStatisticsConfiguration jmxStatistics = configuration.jmxStatistics();
      boolean statisticsAvailable = jmxStatistics != null && jmxStatistics.available();
      if (statisticsAvailable) {
         cache = buildEncodingCache((kc, vc) -> new StatsCollectingCache<>(cacheName, kc, vc), configuration);
      } else {
         cache = buildEncodingCache((kc, vc) -> new SimpleCacheImpl<>(cacheName, kc, vc), configuration);
      }
      this.configuration = configuration;

      componentRegistry = new ComponentRegistry(cacheName, configuration, cache, globalComponentRegistry,
                                                globalComponentRegistry.getClassLoader()) {
         @Override
         protected void bootstrapComponents() {
            registerComponent(new ClusterEventManagerStub<K, V>(), ClusterEventManager.class);
            registerComponent(new PassivationManagerStub(), PassivationManager.class);
            registerComponent(new ActivationManagerStub(), ActivationManager.class);
            registerComponent(new PersistenceManagerStub(), PersistenceManager.class);
         }

         @Override
         public void cacheComponents() {
            getOrCreateComponent(InternalExpirationManager.class);
         }
      };

      basicComponentRegistry = componentRegistry.getComponent(BasicComponentRegistry.class);
      basicComponentRegistry.registerAlias(Cache.class.getName(), AdvancedCache.class.getName(), AdvancedCache.class);
      basicComponentRegistry.registerComponent(AdvancedCache.class, cache, false);

      componentRegistry.registerComponent(new CacheJmxRegistration(), CacheJmxRegistration.class);
      componentRegistry.registerComponent(new RollingUpgradeManager(), RollingUpgradeManager.class);

      return cache;
   }


   /**
    * Bootstraps this factory with a Configuration and a ComponentRegistry.
    */
   private void bootstrap(String cacheName, AdvancedCache<?, ?> cache, Configuration configuration,
                          GlobalComponentRegistry globalComponentRegistry, StreamingMarshaller globalMarshaller) {
      this.configuration = configuration;

      // injection bootstrap stuff
      componentRegistry = new ComponentRegistry(cacheName, configuration, cache, globalComponentRegistry, globalComponentRegistry.getClassLoader());

      EncoderRegistry encoderRegistry = globalComponentRegistry.getComponent(EncoderRegistry.class);

      // Wraps the compatibility marshaller so that it can be used as a transcoder
      if (configuration.compatibility().enabled() && configuration.compatibility().marshaller() != null) {
         Marshaller marshaller = configuration.compatibility().marshaller();
         componentRegistry.wireDependencies(marshaller);
         if (!encoderRegistry.isConversionSupported(MediaType.APPLICATION_OBJECT, marshaller.mediaType())) {
            encoderRegistry.registerTranscoder(new TranscoderMarshallerAdapter(marshaller));
         }
      }

      // Wraps the GlobalMarshaller so that it can be used as a transcoder
      encoderRegistry.registerTranscoder(new TranscoderMarshallerAdapter(globalMarshaller));

      /*
         --------------------------------------------------------------------------------------------------------------
         This is where the bootstrap really happens.  Registering the cache in the component registry will cause
         the component registry to look at the cache's @Inject methods, and construct various components and their
         dependencies, in turn.
         --------------------------------------------------------------------------------------------------------------
       */
      basicComponentRegistry = componentRegistry.getComponent(BasicComponentRegistry.class);
      basicComponentRegistry.registerAlias(Cache.class.getName(), AdvancedCache.class.getName(), AdvancedCache.class);
      basicComponentRegistry.registerComponent(AdvancedCache.class.getName(), cache, false);

      componentRegistry.registerComponent(new CacheJmxRegistration(), CacheJmxRegistration.class.getName(), true);
      if (configuration.transaction().recovery().enabled()) {
         componentRegistry.registerComponent(new RecoveryAdminOperations(), RecoveryAdminOperations.class.getName(), true);
      }
      if (configuration.sites().hasEnabledBackups()) {
         componentRegistry.registerComponent(new XSiteAdminOperations(), XSiteAdminOperations.class.getName(), true);
      }
      // The RollingUpgradeManager should always be added so it is registered in JMX.
      componentRegistry.registerComponent(new RollingUpgradeManager(), RollingUpgradeManager.class.getName(), true);
   }

   @Override
   public Object construct(String componentName) {
      throw new UnsupportedOperationException("Should never be invoked - this is a bootstrap factory.");
   }

   private static void assertKeyNotNull(Object key) {
      requireNonNull(key, "Null keys are not supported!");
   }

   private static void checkCanRun(Cache<?, ?> cache, String cacheName) {
      ComponentStatus status = cache.getStatus();
      if (status == ComponentStatus.FAILED || status == ComponentStatus.TERMINATED) {
         throw log.cacheIsTerminated(cacheName, status.toString());
      }
   }

   private static abstract class AbstractGetAdvancedCache<K, V, T extends AbstractGetAdvancedCache<K, V, T>> extends AbstractDelegatingAdvancedCache<K, V> {

      @Inject
      protected ComponentRegistry componentRegistry;

      public AbstractGetAdvancedCache(AdvancedCache<K, V> cache, AdvancedCacheWrapper<K, V> wrapper) {
         super(cache, wrapper);
      }

      /**
       * This method is for additional components that need to be wired after the field wiring is complete
       */
      @Inject
      public void wireRealCache() {
         // Wire the cache to ensure all components are ready
         componentRegistry.wireDependencies(cache);
      }

      /**
       * This is to be extended when an entry is injected via component registry and you have to do it manually when a
       * new delegating cache is created due to methods like {@link AdvancedCache#withFlags(Flag...)}. This method will
       * call {@link #wireRealCache()} at the very end - so all methods should be initialized before.
       *
       * @param cache
       */
      protected void internalWire(T cache) {
         componentRegistry = cache.componentRegistry;
         wireRealCache();
      }

      @Override
      public V get(Object key) {
         assertKeyNotNull(key);
         checkCanRun(cache, cache.getName());
         InternalCacheEntry<K, V> ice = getDataContainer().get(key);
         if (ice != null) {
            return ice.getValue();
         }
         return null;
      }

      @Override
      public V getOrDefault(Object key, V defaultValue) {
         V value = get(key);
         return value != null ? value : defaultValue;
      }

      @Override
      public boolean containsKey(Object key) {
         return get(key) != null;
      }

      @Override
      public CompletableFuture<V> getAsync(K key) {
         try {
            return CompletableFuture.completedFuture(get(key));
         } catch (Throwable t) {
            return CompletableFutures.completedExceptionFuture(t);
         }
      }
   }

   private static class PartitionHandlingCache<K, V> extends AbstractGetAdvancedCache<K, V, PartitionHandlingCache<K, V>> {
      @Inject
      private PartitionHandlingManager manager;

      // We store the flags as bits passed from AdvancedCache.withFlags etc.
      private final long bitFlags;

      public PartitionHandlingCache(AbstractGetAdvancedCache<K, V, ?> cache) {
         this(cache, 0L);
      }

      private PartitionHandlingCache(AdvancedCache<K, V> cache, long bitFlags) {
         super(cache, new AdvancedCacheWrapper<K, V>() {
            @Override
            public AdvancedCache<K, V> wrap(AdvancedCache<K, V> cache) {
               throw new UnsupportedOperationException();
            }

            @Override
            public AdvancedCache<K, V> wrap(AdvancedCache<K, V> self, AdvancedCache<K, V> newDelegate) {
               PartitionHandlingCache<K, V> prev = (PartitionHandlingCache<K, V>) self;
               PartitionHandlingCache<K, V> newCache = new PartitionHandlingCache<>(newDelegate, prev.bitFlags);
               newCache.internalWire(prev);
               return newCache;
            }
         });
         this.bitFlags = bitFlags;
      }

      @Override
      protected void internalWire(PartitionHandlingCache<K, V> cache) {
         manager = cache.manager;
         super.internalWire(cache);
      }

      @Override
      public V get(Object key) {
         V value = cache.get(key);
         if (!EnumUtil.containsAny(bitFlags, FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.SKIP_OWNERSHIP_CHECK)) {
            manager.checkRead(key, bitFlags);
         }
         return value;
      }

      @Override
      public AdvancedCache<K, V> withFlags(Flag... flags) {
         long newFlags = EnumUtil.bitSetOf(flags);
         long updatedFlags = EnumUtil.mergeBitSets(bitFlags, newFlags);
         if (bitFlags != updatedFlags) {
            PartitionHandlingCache<K, V> newCache = new PartitionHandlingCache<>(super.withFlags(flags), updatedFlags);
            newCache.internalWire(this);
            return newCache;
         }
         return this;
      }

      @Override
      public AdvancedCache<K, V> withFlags(Collection<Flag> flags) {
         long newFlags = EnumUtil.bitSetOf(flags);
         long updatedFlags = EnumUtil.mergeBitSets(bitFlags, newFlags);
         if (bitFlags != updatedFlags) {
            PartitionHandlingCache<K, V> newCache = new PartitionHandlingCache<>(super.withFlags(flags), updatedFlags);
            newCache.internalWire(this);
            return newCache;
         }
         return this;
      }

      @Override
      public AdvancedCache<K, V> noFlags() {
         if (bitFlags != 0) {
            PartitionHandlingCache<K, V> newCache = new PartitionHandlingCache<>(super.noFlags(), 0L);
            newCache.internalWire(this);
            return newCache;
         }
         return this;
      }
   }

   private static class StatsCache<K, V> extends AbstractGetAdvancedCache<K, V, StatsCache<K, V>> {

      @Inject
      private TimeService timeService;
      private CacheMgmtInterceptor interceptor;

      public StatsCache(AbstractGetAdvancedCache<K, V, ?> cache) {
         this((AdvancedCache<K, V>) cache);
      }

      private StatsCache(AdvancedCache<K, V> cache) {
         super(cache, new AdvancedCacheWrapper<K, V>() {
            @Override
            public AdvancedCache<K, V> wrap(AdvancedCache<K, V> cache) {
               throw new UnsupportedOperationException();
            }

            @Override
            public AdvancedCache<K, V> wrap(AdvancedCache<K, V> self, AdvancedCache<K, V> newDelegate) {
               StatsCache<K, V> newCache = new StatsCache<>(newDelegate);
               newCache.internalWire((StatsCache<K, V>) self);
               newCache.interceptorStart();
               return newCache;
            }
         });
      }

      @Override
      protected void internalWire(StatsCache<K, V> cache) {
         this.timeService = cache.timeService;
         super.internalWire(cache);
      }

      private void interceptorStart() {
         // This has to be done after the cache is wired - otherwise we can get a circular dependency
         interceptor = cache.getAsyncInterceptorChain().findInterceptorWithClass(CacheMgmtInterceptor.class);
      }

      @Override
      public V get(Object key) {
         V value;
         if (interceptor == null) {
            interceptorStart();
         }
         if (interceptor.getStatisticsEnabled()) {
            long beginTime = timeService.time();
            value = cache.get(key);
            interceptor.addDataRead(value != null, timeService.timeDuration(beginTime, TimeUnit.NANOSECONDS));
         } else {
            value = cache.get(key);
         }
         return value;
      }
   }

   private static class GetReplCache<K, V> extends AbstractGetAdvancedCache<K, V, GetReplCache<K, V>> {

      @Inject
      private CacheNotifier<K, V> cacheNotifier;

      // The hasListeners is commented out until EncoderCache can properly pass down the addListener invocation
      // to the next delegate in the chain. Otherwise we miss listener additions and don't fire events properly.
      // This is detailed in https://issues.jboss.org/browse/ISPN-9240
//      private final AtomicBoolean hasListeners;
//
//      GetReplCache(AdvancedCache<K, V> cache) {
//         this(cache, new AtomicBoolean());
//      }

      private GetReplCache(AdvancedCache<K, V> cache/*, AtomicBoolean hasListeners*/) {
         super(cache, new AdvancedCacheWrapper<K, V>() {
            @Override
            public AdvancedCache<K, V> wrap(AdvancedCache<K, V> cache) {
               throw new UnsupportedOperationException();
            }

            @Override
            public AdvancedCache<K, V> wrap(AdvancedCache<K, V> self, AdvancedCache<K, V> newDelegate) {
               GetReplCache<K, V> oldCache = (GetReplCache<K, V>) self;
               GetReplCache<K, V> newCache = new GetReplCache<K, V>(newDelegate/*, oldCache.hasListeners*/);
               newCache.internalWire(oldCache);
               return newCache;
            }
         });
//         this.hasListeners = hasListeners;
      }

      @Override
      protected void internalWire(GetReplCache<K, V> cache) {
         cacheNotifier = cache.cacheNotifier;
         super.internalWire(cache);
      }

//      private boolean canFire(Object listener) {
//         for (Method m : listener.getClass().getMethods()) {
//            // Visitor listeners are very rare, so optimize to not call when we don't have any registered
//            if (m.isAnnotationPresent(CacheEntryVisited.class)) {
//               return true;
//            }
//         }
//         return false;
//      }
//
//      @Override
//      public void addListener(Object listener, KeyFilter<? super K> filter) {
//         super.addListener(listener, filter);
//         if (!hasListeners.get() && canFire(listener)) {
//            hasListeners.set(true);
//         }
//      }
//
//      @Override
//      public <C> void addListener(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter) {
//         super.addListener(listener, filter, converter);
//         if (!hasListeners.get() && canFire(listener)) {
//            hasListeners.set(true);
//         }
//      }
//
//      @Override
//      public void addListener(Object listener) {
//         super.addListener(listener);
//         if (!hasListeners.get() && canFire(listener)) {
//            hasListeners.set(true);
//         }
//      }
//
//      @Override
//      public <C> void addFilteredListener(Object listener, CacheEventFilter<? super K, ? super V> filter,
//            CacheEventConverter<? super K, ? super V, C> converter, Set<Class<? extends Annotation>> filterAnnotations) {
//         super.addFilteredListener(listener, filter, converter, filterAnnotations);
//         if (!hasListeners.get() && canFire(listener)) {
//            hasListeners.set(true);
//         }
//      }

      @Override
      public V get(Object key) {
         V value = super.get(key);
         if (value != null/* && hasListeners.get()*/) {
            cacheNotifier.notifyCacheEntryVisited((K) key, value, true, ImmutableContext.INSTANCE, null);
            cacheNotifier.notifyCacheEntryVisited((K) key, value, false, ImmutableContext.INSTANCE, null);
         }
         return value;
      }
   }
}
