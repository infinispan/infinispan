package org.infinispan.factories;

import static java.util.Objects.requireNonNull;
import static org.infinispan.encoding.DataConversion.newKeyDataConversion;
import static org.infinispan.encoding.DataConversion.newValueDataConversion;
import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingAdvancedCache;
import org.infinispan.cache.impl.CacheImpl;
import org.infinispan.cache.impl.EncoderCache;
import org.infinispan.cache.impl.StatsCollectingCache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.ImmutableContext;
import org.infinispan.distribution.Ownership;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.eviction.impl.PassivationManager;
import org.infinispan.eviction.impl.PassivationManagerStub;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.CacheMgmtInterceptor;
import org.infinispan.jmx.CacheJmxRegistration;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.metrics.impl.CacheMetricsRegistration;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventManager;
import org.infinispan.notifications.cachelistener.cluster.impl.ClusterEventManagerStub;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.transaction.xa.recovery.RecoveryAdminOperations;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.infinispan.xsite.XSiteAdminOperations;

/**
 * An internal factory for constructing Caches.  Used by the {@link org.infinispan.manager.DefaultCacheManager}, this is
 * not intended as public API.
  * This is a special instance of a {@link AbstractComponentFactory} which contains bootstrap information for the {@link
 * ComponentRegistry}.
  *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @since 4.0
 */
public class InternalCacheFactory<K, V> {

   private ComponentRegistry componentRegistry;
   private BasicComponentRegistry basicComponentRegistry;

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

   private AdvancedCache<K, V> createAndWire(Configuration configuration,
                                             GlobalComponentRegistry globalComponentRegistry,
                                             String cacheName) {
      Marshaller marshaller = globalComponentRegistry.getOrCreateComponent(Marshaller.class,
                                                                                    KnownComponentNames.INTERNAL_MARSHALLER);

      AdvancedCache<K, V> cache = new CacheImpl<>(cacheName);
      // We can optimize REPL reads that meet some criteria. This allows us to bypass interceptor chain
      if (configuration.clustering().cacheMode().isReplicated() && !configuration.persistence().usingStores()
          && !configuration.transaction().transactionMode().isTransactional() &&
          configuration.clustering().stateTransfer().awaitInitialTransfer() &&
          configuration.clustering().hash().capacityFactor() != 0f &&
          !globalComponentRegistry.getGlobalConfiguration().isZeroCapacityNode()) {
         cache = new GetReplCache<>(new CacheImpl<>(cacheName));
         cache = new StatsCache<>(cache);
         if (configuration.clustering().partitionHandling().whenSplit() != PartitionHandling.ALLOW_READ_WRITES) {
            cache = new PartitionHandlingCache<>(cache);
         }
      }

      AdvancedCache<K, V> encodedCache = buildEncodingCache(cache);

      // TODO Register the cache without encoding in the component registry
      bootstrap(cacheName, encodedCache, configuration, globalComponentRegistry);
      if (marshaller != null) {
         componentRegistry.wireDependencies(marshaller, false);
      }
      return encodedCache;
   }

   private AdvancedCache<K, V> buildEncodingCache(AdvancedCache<K, V> wrappedCache) {
      DataConversion keyDataConversion = newKeyDataConversion();
      DataConversion valueDataConversion = newValueDataConversion();

      return new EncoderCache<>(wrappedCache, null, null, keyDataConversion, valueDataConversion);
   }

   private AdvancedCache<K, V> createSimpleCache(Configuration configuration,
                                                 GlobalComponentRegistry globalComponentRegistry,
                                                 String cacheName) {
      AdvancedCache<K, V> cache = buildEncodingCache(new StatsCollectingCache<>(cacheName));

      componentRegistry = new SimpleComponentRegistry<>(cacheName, configuration, cache, globalComponentRegistry);

      basicComponentRegistry = componentRegistry.getComponent(BasicComponentRegistry.class);
      basicComponentRegistry.registerAlias(Cache.class.getName(), AdvancedCache.class.getName(), AdvancedCache.class);
      basicComponentRegistry.registerComponent(AdvancedCache.class, cache, false);

      componentRegistry.registerComponent(new CacheJmxRegistration(), CacheJmxRegistration.class);
      componentRegistry.registerComponent(new CacheMetricsRegistration(), CacheMetricsRegistration.class);
      componentRegistry.registerComponent(new RollingUpgradeManager(), RollingUpgradeManager.class);

      return cache;
   }

   /**
    * Bootstraps this factory with a Configuration and a ComponentRegistry.
    */
   private void bootstrap(String cacheName, AdvancedCache<?, ?> cache, Configuration configuration,
                          GlobalComponentRegistry globalComponentRegistry) {
      // injection bootstrap stuff
      componentRegistry = new ComponentRegistry(cacheName, configuration, cache, globalComponentRegistry, globalComponentRegistry.getClassLoader());

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
      componentRegistry.registerComponent(new CacheMetricsRegistration(), CacheMetricsRegistration.class.getName(), true);
      if (configuration.transaction().recovery().enabled()) {
         componentRegistry.registerComponent(new RecoveryAdminOperations(), RecoveryAdminOperations.class.getName(), true);
      }
      if (configuration.sites().hasBackups()) {
         componentRegistry.registerComponent(new XSiteAdminOperations(), XSiteAdminOperations.class.getName(), true);
      }
      // The RollingUpgradeManager should always be added so it is registered in JMX.
      componentRegistry.registerComponent(new RollingUpgradeManager(), RollingUpgradeManager.class.getName(), true);
   }

   private static void assertKeyNotNull(Object key) {
      requireNonNull(key, "Null keys are not supported!");
   }

   private static void checkCanRun(Cache<?, ?> cache, String cacheName) {
      ComponentStatus status = cache.getStatus();
      if (status == ComponentStatus.FAILED || status == ComponentStatus.TERMINATED) {
         throw CONTAINER.cacheIsTerminated(cacheName, status.toString());
      }
   }

   @Scope(Scopes.NAMED_CACHE)
   static abstract class AbstractGetAdvancedCache<K, V, T extends AbstractGetAdvancedCache<K, V, T>>
         extends AbstractDelegatingAdvancedCache<K, V> {

      @Inject
      protected ComponentRegistry componentRegistry;
      @Inject
      InternalExpirationManager<K, V> expirationManager;
      @Inject
      KeyPartitioner keyPartitioner;

      public AbstractGetAdvancedCache(AdvancedCache<K, V> cache) {
         super(cache);
      }

      /**
       * This method is for additional components that need to be wired after the field wiring is complete
       */
      @Inject
      public void wireRealCache() {
         // Wire the cache to ensure all components are ready
         componentRegistry.wireDependencies(cache, false);
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
         expirationManager = cache.expirationManager;
         keyPartitioner = cache.keyPartitioner;
         wireRealCache();
      }

      @Override
      public InternalDataContainer<K, V> getDataContainer() {
         return (InternalDataContainer<K, V>) super.getDataContainer();
      }

      @Override
      public V get(Object key) {
         InternalCacheEntry<K, V> ice = getCacheEntry(key);
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
      public InternalCacheEntry<K, V> getCacheEntry(Object key) {
         assertKeyNotNull(key);
         checkCanRun(cache, cache.getName());
         int segment = keyPartitioner.getSegment(key);
         InternalCacheEntry<K, V> ice = getDataContainer().peek(segment, key);
         if (ice != null && ice.canExpire()) {
            CompletionStage<Boolean> stage = expirationManager.handlePossibleExpiration(ice, segment, false);
            if (CompletionStages.join(stage)) {
               ice = null;
            }
         }
         return ice;
      }
   }

   static class PartitionHandlingCache<K, V> extends AbstractGetAdvancedCache<K, V, PartitionHandlingCache<K, V>> {
      @Inject PartitionHandlingManager manager;

      // We store the flags as bits passed from AdvancedCache.withFlags etc.
      private final long bitFlags;

      public PartitionHandlingCache(AdvancedCache<K, V> cache) {
         this(cache, 0L);
      }

      private PartitionHandlingCache(AdvancedCache<K, V> cache, long bitFlags) {
         super(cache);
         this.bitFlags = bitFlags;
      }

      @Override
      public AdvancedCache rewrap(AdvancedCache newDelegate) {
         PartitionHandlingCache newCache = new PartitionHandlingCache<>(newDelegate, this.bitFlags);
         newCache.internalWire(this);
         return newCache;
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

   static class StatsCache<K, V> extends AbstractGetAdvancedCache<K, V, StatsCache<K, V>> {

      @Inject TimeService timeService;
      private CacheMgmtInterceptor interceptor;

      public StatsCache(AdvancedCache<K, V> cache) {
         super(cache);
      }

      @Override
      public AdvancedCache rewrap(AdvancedCache newDelegate) {
         StatsCache newCache = new StatsCache<>(newDelegate);
         newCache.internalWire(this);
         newCache.interceptorStart();
         return newCache;
      }

      @Override
      protected void internalWire(StatsCache<K, V> cache) {
         this.timeService = cache.timeService;
         super.internalWire(cache);
      }

      private void interceptorStart() {
         // This has to be done after the cache is wired - otherwise we can get a circular dependency
         interceptor =  ComponentRegistry.componentOf(cache, AsyncInterceptorChain.class).findInterceptorWithClass(CacheMgmtInterceptor.class);
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
            interceptor.addDataRead(value != null, beginTime, Ownership.PRIMARY);
         } else {
            value = cache.get(key);
         }
         return value;
      }
   }

   static class GetReplCache<K, V> extends AbstractGetAdvancedCache<K, V, GetReplCache<K, V>> {

      @Inject CacheNotifier<K, V> cacheNotifier;

      // The hasListeners is commented out until EncoderCache can properly pass down the addListener invocation
      // to the next delegate in the chain. Otherwise we miss listener additions and don't fire events properly.
      // This is detailed in https://issues.jboss.org/browse/ISPN-9240
//      private final AtomicBoolean hasListeners;
//
//      GetReplCache(AdvancedCache<K, V> cache) {
//         this(cache, new AtomicBoolean());
//      }

      private GetReplCache(AdvancedCache<K, V> cache/*, AtomicBoolean hasListeners*/) {
         super(cache);
//         this.hasListeners = hasListeners;
      }

      @Override
      public AdvancedCache rewrap(AdvancedCache newDelegate) {
         GetReplCache newCache = new GetReplCache<>(newDelegate/*, oldCache.hasListeners*/);
         newCache.internalWire(this);
         return newCache;
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
            CompletionStages.join(cacheNotifier.notifyCacheEntryVisited((K) key, value, true, ImmutableContext.INSTANCE, null));
            CompletionStages.join(cacheNotifier.notifyCacheEntryVisited((K) key, value, false, ImmutableContext.INSTANCE, null));
         }
         return value;
      }
   }

   @SurvivesRestarts
   static
   class SimpleComponentRegistry<K, V> extends ComponentRegistry {
      public SimpleComponentRegistry(String cacheName, Configuration configuration, AdvancedCache<K, V> cache,
                                     GlobalComponentRegistry globalComponentRegistry) {
         super(cacheName, configuration, cache, globalComponentRegistry, globalComponentRegistry.getClassLoader());
      }

      @Override
      protected void bootstrapComponents() {
         registerComponent(new ClusterEventManagerStub<K, V>(), ClusterEventManager.class);
         registerComponent(new PassivationManagerStub(), PassivationManager.class);
         registerComponent(new InternalCacheFactory<>(), InternalCacheFactory.class);
      }

      @Override
      public void cacheComponents() {
         getOrCreateComponent(InternalExpirationManager.class);
         internalEntryFactory = basicComponentRegistry.getComponent(InternalEntryFactory.class);
      }
   }
}
