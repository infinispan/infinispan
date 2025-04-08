package org.infinispan.hibernate.cache.v62.impl;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.hibernate.cache.cfg.spi.CollectionDataCachingConfig;
import org.hibernate.cache.cfg.spi.DomainDataCachingConfig;
import org.hibernate.cache.cfg.spi.DomainDataRegionConfig;
import org.hibernate.cache.cfg.spi.EntityDataCachingConfig;
import org.hibernate.cache.cfg.spi.NaturalIdDataCachingConfig;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.cache.spi.DomainDataRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.metamodel.model.domain.NavigableRole;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.expiration.impl.ClusterExpirationManager;
import org.infinispan.expiration.impl.ExpirationManagerImpl;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.functional.MetaParam;
import org.infinispan.hibernate.cache.commons.InfinispanDataRegion;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;
import org.infinispan.hibernate.cache.commons.access.LockingInterceptor;
import org.infinispan.hibernate.cache.commons.access.PutFromLoadValidator;
import org.infinispan.hibernate.cache.commons.access.UnorderedDistributionInterceptor;
import org.infinispan.hibernate.cache.commons.access.UnorderedReplicationLogic;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.hibernate.cache.commons.util.Tombstone;
import org.infinispan.hibernate.cache.commons.util.VersionedEntry;
import org.infinispan.hibernate.cache.v62.InfinispanRegionFactory;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.distribution.NonTxDistributionInterceptor;
import org.infinispan.interceptors.distribution.TriangleDistributionInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;

/**
 * @author Chris Bredesen
 * @author Galder Zamarreño
 * @since 3.5
 */
public class DomainDataRegionImpl
      extends BaseRegionImpl implements DomainDataRegion, InfinispanDataRegion {
   private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(DomainDataRegionImpl.class);
   private final CacheKeysFactory cacheKeysFactory;
   private final DomainDataRegionConfig config;
   private final AdvancedCache<Object, Object> localCache;
   private final Map<String, Comparator<Object>> comparatorsByType = new HashMap<>();

   private final long tombstoneExpiration;
   private PutFromLoadValidator validator;

   private Strategy strategy;
   private final MetaParam.MetaLifespan expiringMetaParam;

   protected enum Strategy {
      NONE, VALIDATION, TOMBSTONES, VERSIONED_ENTRIES
   }

   public DomainDataRegionImpl(AdvancedCache cache, DomainDataRegionConfig config,
         InfinispanRegionFactory factory, CacheKeysFactory cacheKeysFactory) {
      super(cache, config.getRegionName(), factory);
      this.config = config;
      this.cacheKeysFactory = cacheKeysFactory;
      localCache = cache.withFlags(Flag.CACHE_MODE_LOCAL);

      tombstoneExpiration = factory.getPendingPutsCacheConfiguration().expiration().maxIdle();
      expiringMetaParam = new MetaParam.MetaLifespan(tombstoneExpiration);
   }

   @Override
   public EntityDataAccess getEntityDataAccess(NavigableRole rootEntityRole) {
      EntityDataCachingConfig entityConfig = findConfig(config.getEntityCaching(), rootEntityRole);
      AccessType accessType = entityConfig.getAccessType();
      AccessDelegate accessDelegate = createAccessDelegate(accessType,
            entityConfig.isVersioned() ? entityConfig.getVersionComparatorAccess().get() : null);
      if (accessType == AccessType.READ_ONLY || !entityConfig.isMutable()) {
         return new ReadOnlyEntityDataAccess(accessType, accessDelegate, this);
      } else {
         return new ReadWriteEntityDataAccess(accessType, accessDelegate, this);
      }
   }

   @Override
   public NaturalIdDataAccess getNaturalIdDataAccess(NavigableRole rootEntityRole) {
      NaturalIdDataCachingConfig naturalIdConfig = findConfig(this.config.getNaturalIdCaching(), rootEntityRole);
      AccessType accessType = naturalIdConfig.getAccessType();
      if (accessType == AccessType.NONSTRICT_READ_WRITE) {
         // We don't support nonstrict read write for natural ids as NSRW requires versions;
         // natural ids aren't versioned by definition (as the values are primary keys).
         accessType = AccessType.READ_WRITE;
      }
      AccessDelegate accessDelegate = createAccessDelegate(accessType, null);
      if (accessType == AccessType.READ_ONLY || !naturalIdConfig.isMutable()) {
         return new ReadOnlyNaturalDataAccess(accessType, accessDelegate, this);
      } else {
         return new ReadWriteNaturalDataAccess(accessType, accessDelegate, this);
      }
   }

   @Override
   public CollectionDataAccess getCollectionDataAccess(NavigableRole collectionRole) {
      CollectionDataCachingConfig collectionConfig = findConfig(this.config.getCollectionCaching(), collectionRole);
      AccessType accessType = collectionConfig.getAccessType();
      AccessDelegate accessDelegate = createAccessDelegate(accessType, collectionConfig.getOwnerVersionComparator());
      return new CollectionDataAccessImpl(this, accessDelegate, accessType);
   }

   private <T extends DomainDataCachingConfig> T findConfig(List<T> configs, NavigableRole role) {
      return configs.stream()
            .filter(c -> c.getNavigableRole().equals(role))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Cannot find configuration for " + role));
   }

   public CacheKeysFactory getCacheKeysFactory() {
      return cacheKeysFactory;
   }

   private synchronized AccessDelegate createAccessDelegate(AccessType accessType, Comparator<Object> comparator) {
      CacheMode cacheMode = cache.getCacheConfiguration().clustering().cacheMode();
      if (accessType == AccessType.NONSTRICT_READ_WRITE) {
         prepareForVersionedEntries(cacheMode);
         return new NonStrictAccessDelegate(this, comparator);
      }
      if (cacheMode.isDistributed() || cacheMode.isReplicated()) {
         prepareForTombstones(cacheMode);
         return new TombstoneAccessDelegate(this);
      } else {
         prepareForValidation();
         return new NonTxInvalidationCacheAccessDelegate(this, validator);
      }
   }

   private void prepareForValidation() {
      if (strategy != null) {
         assert strategy == Strategy.VALIDATION;
         return;
      }
      validator = new PutFromLoadValidator(cache, factory, factory.getPendingPutsCacheConfiguration());
      strategy = Strategy.VALIDATION;
   }

   private void prepareForVersionedEntries(CacheMode cacheMode) {
      if (strategy != null) {
         assert strategy == Strategy.VERSIONED_ENTRIES;
         return;
      }

      prepareCommon(cacheMode);
      filter = VersionedEntry.EXCLUDE_EMPTY_VERSIONED_ENTRY;

      for (EntityDataCachingConfig entityConfig : config.getEntityCaching()) {
         if (entityConfig.isVersioned()) {
            for (NavigableRole role : entityConfig.getCachedTypes()) {
               comparatorsByType.put(role.getNavigableName(), entityConfig.getVersionComparatorAccess().get());
            }
         }
      }
      for (CollectionDataCachingConfig collectionConfig : config.getCollectionCaching()) {
         if (collectionConfig.isVersioned()) {
            comparatorsByType.put(collectionConfig.getNavigableRole().getNavigableName(), collectionConfig.getOwnerVersionComparator());
         }
      }

      strategy = Strategy.VERSIONED_ENTRIES;
   }

   private void prepareForTombstones(CacheMode cacheMode) {
      if (strategy != null) {
         assert strategy == Strategy.TOMBSTONES;
         return;
      }
      Configuration configuration = cache.getCacheConfiguration();
      if (configuration.memory().isEvictionEnabled()) {
         log.evictionWithTombstones();
      }

      prepareCommon(cacheMode);
      filter = Tombstone.EXCLUDE_TOMBSTONES;

      strategy = Strategy.TOMBSTONES;
   }

   private void prepareCommon(CacheMode cacheMode) {
      BasicComponentRegistry registry = ComponentRegistry.componentOf(cache, BasicComponentRegistry.class);
      if (cacheMode.isReplicated() || cacheMode.isDistributed()) {
         AsyncInterceptorChain chain = ComponentRegistry.componentOf(cache, AsyncInterceptorChain.class);

         LockingInterceptor lockingInterceptor = new LockingInterceptor();
         registry.registerComponent(LockingInterceptor.class, lockingInterceptor, true);
         registry.getComponent(LockingInterceptor.class).running();
         if (!chain.addInterceptorBefore(lockingInterceptor, NonTransactionalLockingInterceptor.class)) {
            throw new IllegalStateException("Misconfigured cache, interceptor chain is " + chain);
         }
         chain.removeInterceptor(NonTransactionalLockingInterceptor.class);

         UnorderedDistributionInterceptor distributionInterceptor = new UnorderedDistributionInterceptor();
         registry.registerComponent(UnorderedDistributionInterceptor.class, distributionInterceptor, true);
         registry.getComponent(UnorderedDistributionInterceptor.class).running();
         if (!chain.addInterceptorBefore(distributionInterceptor, NonTxDistributionInterceptor.class) &&
               !chain.addInterceptorBefore(distributionInterceptor, TriangleDistributionInterceptor.class)) {
            throw new IllegalStateException("Misconfigured cache, interceptor chain is " + chain);
         }

         chain.removeInterceptor(NonTxDistributionInterceptor.class);
         chain.removeInterceptor(TriangleDistributionInterceptor.class);
      }

      // ClusteredExpirationManager sends RemoteExpirationCommands to remote nodes which causes
      // undesired overhead. When get() triggers a RemoteExpirationCommand executed in async executor
      // this locks the entry for the duration of RPC, and putFromLoad with ZERO_LOCK_ACQUISITION_TIMEOUT
      // fails as it finds the entry being blocked.
      ComponentRef<InternalExpirationManager> ref = registry.getComponent(InternalExpirationManager.class);
      InternalExpirationManager expirationManager = ref.running();
      if (expirationManager instanceof ClusterExpirationManager) {
         registry.replaceComponent(InternalExpirationManager.class.getName(), new ExpirationManagerImpl<>(), true);
         registry.getComponent(InternalExpirationManager.class).running();
         registry.rewire();
         // re-registering component does not stop the old one
         ((ClusterExpirationManager) expirationManager).stop();
      } else if (expirationManager instanceof ExpirationManagerImpl) {
         // do nothing
      } else {
         throw new IllegalStateException("Expected clustered expiration manager, found " + expirationManager);
      }

      registry.registerComponent(InfinispanDataRegion.class, this, true);

      if (cacheMode.isClustered()) {
         UnorderedReplicationLogic replLogic = new UnorderedReplicationLogic();
         registry.replaceComponent(ClusteringDependentLogic.class.getName(), replLogic, true);
         registry.getComponent(ClusteringDependentLogic.class).running();
         registry.rewire();
      }
   }

   public long getTombstoneExpiration() {
      return tombstoneExpiration;
   }

   @Override
   public MetaParam.MetaLifespan getExpiringMetaParam() {
      return expiringMetaParam;
   }

   @Override
   public Comparator<Object> getComparator(String subclass) {
      return comparatorsByType.get(subclass);
   }

   @Override
   protected void runInvalidation() {
      if (strategy == null) {
         throw new IllegalStateException("Strategy was not set");
      }
      switch (strategy) {
         case NONE:
         case VALIDATION:
            super.runInvalidation();
            return;
         case TOMBSTONES:
            removeEntries(entry -> localCache.remove(entry.getKey(), entry.getValue()));
            return;
         case VERSIONED_ENTRIES:
            // no need to use this as a function - simply override all
            VersionedEntry evict = new VersionedEntry(factory.nextTimestamp());
            removeEntries(entry -> localCache.put(entry.getKey(), evict, tombstoneExpiration, TimeUnit.MILLISECONDS));
            return;
      }
   }

   private void removeEntries(Consumer<Map.Entry<Object, Object>> remover) {
      // We can never use cache.clear() since tombstones must be kept.
      localCache.entrySet().stream().filter(filter).forEach(remover);
   }

   @Override
   public void destroy() throws CacheException {
      super.destroy();
      if (validator != null) {
         validator.destroy();
      }
   }

   // exposed just to help tests
   public DomainDataRegionConfig config() {
      return config;
   }
}
