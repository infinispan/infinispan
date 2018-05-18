/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.v53.impl;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.expiration.impl.ClusterExpirationManager;
import org.infinispan.expiration.impl.ExpirationManagerImpl;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;
import org.infinispan.hibernate.cache.commons.access.LockingInterceptor;
import org.infinispan.hibernate.cache.commons.access.NonStrictAccessDelegate;
import org.infinispan.hibernate.cache.commons.access.NonTxInvalidationCacheAccessDelegate;
import org.infinispan.hibernate.cache.commons.access.PutFromLoadValidator;
import org.infinispan.hibernate.cache.commons.access.TombstoneAccessDelegate;
import org.infinispan.hibernate.cache.commons.access.TombstoneCallInterceptor;
import org.infinispan.hibernate.cache.commons.access.UnorderedDistributionInterceptor;
import org.infinispan.hibernate.cache.commons.access.UnorderedReplicationLogic;
import org.infinispan.hibernate.cache.commons.access.VersionedCallInterceptor;
import org.infinispan.hibernate.cache.commons.InfinispanDataRegion;
import org.infinispan.hibernate.cache.commons.util.Caches;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.hibernate.cache.commons.util.Tombstone;
import org.infinispan.hibernate.cache.commons.util.VersionedEntry;
import org.infinispan.hibernate.cache.v53.InfinispanRegionFactory;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.distribution.NonTxDistributionInterceptor;
import org.infinispan.interceptors.distribution.TriangleDistributionInterceptor;
import org.infinispan.interceptors.impl.CallInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;

/**
 * @author Chris Bredesen
 * @author Galder Zamarreño
 * @since 3.5
 */
public class DomainDataRegionImpl
      extends BaseRegionImpl implements DomainDataRegion, InfinispanDataRegion {
   private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog( DomainDataRegionImpl.class );
   private final CacheKeysFactory cacheKeysFactory;
   private final DomainDataRegionConfig config;
   private final Map<String, Comparator<Object>> comparatorsByType = new HashMap<>();

   private long tombstoneExpiration;
   private PutFromLoadValidator validator;

   private Strategy strategy;

   protected enum Strategy {
      NONE, VALIDATION, TOMBSTONES, VERSIONED_ENTRIES
   }

   public DomainDataRegionImpl(AdvancedCache cache, DomainDataRegionConfig config,
                               InfinispanRegionFactory factory, CacheKeysFactory cacheKeysFactory) {
      super(cache, config.getRegionName(), factory);
      this.config = config;
      this.cacheKeysFactory = cacheKeysFactory;

      tombstoneExpiration = factory.getPendingPutsCacheConfiguration().expiration().maxIdle();
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
         // We cannot use comparator to initialize the VersionedCallInterceptor as this should accept all possible
         // types that can be stored in the region.
         prepareForVersionedEntries(cacheMode);
         return new NonStrictAccessDelegate(this, comparator);
      }
      if (cacheMode.isDistributed() || cacheMode.isReplicated()) {
         prepareForTombstones();
         return new TombstoneAccessDelegate(this);
      }
      else {
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

      replaceCommonInterceptors();
      replaceExpirationManager();

      VersionedCallInterceptor versionedCallInterceptor = new VersionedCallInterceptor(this);
      ComponentRegistry compReg = cache.getComponentRegistry();
      compReg.registerComponent(versionedCallInterceptor, VersionedCallInterceptor.class);
      AsyncInterceptorChain interceptorChain = cache.getAsyncInterceptorChain();
      interceptorChain.addInterceptorBefore(versionedCallInterceptor, CallInterceptor.class);

      if (cacheMode.isClustered()) {
         UnorderedReplicationLogic replLogic = new UnorderedReplicationLogic();
         compReg.registerComponent( replLogic, ClusteringDependentLogic.class );
         compReg.rewire();
      }

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

   private void prepareForTombstones() {
      if (strategy != null) {
         assert strategy == Strategy.TOMBSTONES;
         return;
      }
      Configuration configuration = cache.getCacheConfiguration();
      if (configuration.memory().isEvictionEnabled()) {
         log.evictionWithTombstones();
      }

      replaceCommonInterceptors();
      replaceExpirationManager();

      TombstoneCallInterceptor tombstoneCallInterceptor = new TombstoneCallInterceptor(this);
      ComponentRegistry compReg = cache.getComponentRegistry();
      compReg.registerComponent( tombstoneCallInterceptor, TombstoneCallInterceptor.class);
      AsyncInterceptorChain interceptorChain = cache.getAsyncInterceptorChain();
      interceptorChain.addInterceptorBefore(tombstoneCallInterceptor, CallInterceptor.class);

      UnorderedReplicationLogic replLogic = new UnorderedReplicationLogic();
      compReg.registerComponent( replLogic, ClusteringDependentLogic.class );
      compReg.rewire();

      strategy = Strategy.TOMBSTONES;
   }

   private void replaceCommonInterceptors() {
      CacheMode cacheMode = cache.getCacheConfiguration().clustering().cacheMode();
      if (!cacheMode.isReplicated() && !cacheMode.isDistributed()) {
         return;
      }

      AsyncInterceptorChain chain = cache.getAsyncInterceptorChain();

      LockingInterceptor lockingInterceptor = new LockingInterceptor();
      cache.getComponentRegistry().registerComponent(lockingInterceptor, LockingInterceptor.class);
      if (!chain.addInterceptorBefore(lockingInterceptor, NonTransactionalLockingInterceptor.class)) {
         throw new IllegalStateException("Misconfigured cache, interceptor chain is " + chain);
      }
      cache.removeInterceptor(NonTransactionalLockingInterceptor.class);

      UnorderedDistributionInterceptor distributionInterceptor = new UnorderedDistributionInterceptor();
      cache.getComponentRegistry().registerComponent(distributionInterceptor, UnorderedDistributionInterceptor.class);
      if (!chain.addInterceptorBefore(distributionInterceptor, NonTxDistributionInterceptor.class) &&
               !chain.addInterceptorBefore( distributionInterceptor, TriangleDistributionInterceptor.class)) {
         throw new IllegalStateException("Misconfigured cache, interceptor chain is " + chain);
      }

      cache.removeInterceptor(NonTxDistributionInterceptor.class);
      cache.removeInterceptor(TriangleDistributionInterceptor.class);
   }

   private void replaceExpirationManager() {
      // ClusteredExpirationManager sends RemoteExpirationCommands to remote nodes which causes
      // undesired overhead. When get() triggers a RemoteExpirationCommand executed in async executor
      // this locks the entry for the duration of RPC, and putFromLoad with ZERO_LOCK_ACQUISITION_TIMEOUT
      // fails as it finds the entry being blocked.
      ExpirationManager expirationManager = cache.getComponentRegistry().getComponent(ExpirationManager.class);
      if ((expirationManager instanceof ClusterExpirationManager)) {
         // re-registering component does not stop the old one
         ((ClusterExpirationManager) expirationManager).stop();
         cache.getComponentRegistry().registerComponent(new ExpirationManagerImpl<>(), ExpirationManager.class);
         cache.getComponentRegistry().rewire();
      }
      else if (expirationManager instanceof ExpirationManagerImpl) {
         // do nothing
      }
      else {
         throw new IllegalStateException("Expected clustered expiration manager, found " + expirationManager);
      }
   }

   public long getTombstoneExpiration() {
      return tombstoneExpiration;
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
            removeEntries(Tombstone.EXCLUDE_TOMBSTONES);
            return;
         case VERSIONED_ENTRIES:
            removeEntries(VersionedEntry.EXCLUDE_EMPTY_EXTRACT_VALUE);
            return;
      }
   }

   private void removeEntries(KeyValueFilter filter) {
      // If the transaction is required, we simply need it -> will create our own
      // We can never use cache.clear() since tombstones must be kept.
      AdvancedCache localCache = Caches.localCache(cache);
      CloseableIterator<CacheEntry> it = Caches.entrySet(localCache, filter).iterator();
      long now = factory.nextTimestamp();
      try {
         while (it.hasNext()) {
            // Cannot use it.next(); it.remove() due to ISPN-5653
            CacheEntry entry = it.next();
            switch (strategy) {
               case TOMBSTONES:
                  localCache.remove(entry.getKey(), entry.getValue());
                  break;
               case VERSIONED_ENTRIES:
                  localCache.put(entry.getKey(), new VersionedEntry(null, null, now), tombstoneExpiration, TimeUnit.MILLISECONDS);
                  break;
            }
         }
      }
      finally {
         it.close();
      }
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
