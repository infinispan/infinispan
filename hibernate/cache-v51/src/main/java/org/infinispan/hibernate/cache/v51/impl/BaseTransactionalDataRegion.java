/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.v51.impl;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.transaction.TransactionManager;

import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.cache.spi.TransactionalDataRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.expiration.impl.ClusterExpirationManager;
import org.infinispan.expiration.impl.ExpirationManagerImpl;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.MetaParam;
import org.infinispan.hibernate.cache.commons.InfinispanDataRegion;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;
import org.infinispan.hibernate.cache.commons.access.LockingInterceptor;
import org.infinispan.hibernate.cache.commons.access.NonStrictAccessDelegate;
import org.infinispan.hibernate.cache.commons.access.NonTxInvalidationCacheAccessDelegate;
import org.infinispan.hibernate.cache.commons.access.PutFromLoadValidator;
import org.infinispan.hibernate.cache.commons.access.TombstoneAccessDelegate;
import org.infinispan.hibernate.cache.commons.access.TxInvalidationCacheAccessDelegate;
import org.infinispan.hibernate.cache.commons.access.UnorderedDistributionInterceptor;
import org.infinispan.hibernate.cache.commons.access.UnorderedReplicationLogic;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.hibernate.cache.commons.util.Tombstone;
import org.infinispan.hibernate.cache.commons.util.VersionedEntry;
import org.infinispan.hibernate.cache.v51.InfinispanRegionFactory;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.distribution.NonTxDistributionInterceptor;
import org.infinispan.interceptors.distribution.TriangleDistributionInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;

/**
 * Support for Inifinispan {@link org.hibernate.cache.spi.TransactionalDataRegion} implementors.
 *
 * @author Chris Bredesen
 * @author Galder Zamarreño
 * @since 3.5
 */
public abstract class BaseTransactionalDataRegion
		extends BaseRegion implements TransactionalDataRegion, InfinispanDataRegion {
	private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog( BaseTransactionalDataRegion.class );
	private final CacheDataDescription metadata;
	private final CacheKeysFactory cacheKeysFactory;
	private final boolean requiresTransaction;
	private final MetaParam.MetaLifespan expiringMetaParam;
	private final AdvancedCache<Object, Object> localCache;

	private long tombstoneExpiration;
	private PutFromLoadValidator validator;

	private AccessType accessType;
	private Strategy strategy;

	protected enum Strategy {
		NONE, VALIDATION, TOMBSTONES, VERSIONED_ENTRIES
	}

	/**
	 * Base transactional region constructor
	 *  @param cache instance to store transactional data
	 * @param name of the transactional region
	 * @param transactionManager
	 * @param metadata for the transactional region
	 * @param factory for the transactional region
	 * @param cacheKeysFactory factory for cache keys
	 */
	public BaseTransactionalDataRegion(
         AdvancedCache cache, String name, TransactionManager transactionManager,
         CacheDataDescription metadata, InfinispanRegionFactory factory, CacheKeysFactory cacheKeysFactory) {
		super( cache, name, transactionManager, factory);
		this.metadata = metadata;
		this.cacheKeysFactory = cacheKeysFactory;
		localCache = cache.withFlags(Flag.CACHE_MODE_LOCAL);

		Configuration configuration = cache.getCacheConfiguration();
		requiresTransaction = configuration.transaction().transactionMode().isTransactional()
				&& !configuration.transaction().autoCommit();
		tombstoneExpiration = factory.getPendingPutsCacheConfiguration().expiration().maxIdle();
		if (!isRegionAccessStrategyEnabled()) {
			strategy = Strategy.NONE;
		}
		expiringMetaParam = new MetaParam.MetaLifespan(tombstoneExpiration);
	}

	/**
	 * @return True if this region is accessed through RegionAccessStrategy, false if it is accessed directly.
    */
	protected boolean isRegionAccessStrategyEnabled() {
		return true;
	}

	@Override
	public CacheDataDescription getCacheDataDescription() {
		return metadata;
	}

	public CacheKeysFactory getCacheKeysFactory() {
		return cacheKeysFactory;
	}

	protected synchronized AccessDelegate createAccessDelegate(AccessType accessType) {
		if (accessType == null) {
			throw new IllegalArgumentException();
		}
		if (this.accessType != null && !this.accessType.equals(accessType)) {
			throw new IllegalStateException("This region was already set up for " + this.accessType + ", cannot use using " + accessType);
		}
		this.accessType = accessType;

		CacheMode cacheMode = cache.getCacheConfiguration().clustering().cacheMode();
		if (accessType == AccessType.NONSTRICT_READ_WRITE) {
			prepareForVersionedEntries(cacheMode);
			return new NonStrictAccessDelegate(this, getCacheDataDescription().getVersionComparator());
		}
		if (cacheMode.isDistributed() || cacheMode.isReplicated()) {
			prepareForTombstones(cacheMode);
			return new TombstoneAccessDelegate(this);
		} else {
			prepareForValidation();
			if (cache.getCacheConfiguration().transaction().transactionMode().isTransactional()) {
				return new TxInvalidationCacheAccessDelegate(this, validator);
			}
			else {
				return new NonTxInvalidationCacheAccessDelegate(this, validator);
			}
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
		ComponentRegistry registry = cache.getComponentRegistry();
		if (cacheMode.isReplicated() || cacheMode.isDistributed()) {
			AsyncInterceptorChain chain = cache.getAsyncInterceptorChain();

			LockingInterceptor lockingInterceptor = new LockingInterceptor();
			registry.registerComponent(lockingInterceptor, LockingInterceptor.class);
			if (!chain.addInterceptorBefore(lockingInterceptor, NonTransactionalLockingInterceptor.class)) {
				throw new IllegalStateException("Misconfigured cache, interceptor chain is " + chain);
			}
			chain.removeInterceptor(NonTransactionalLockingInterceptor.class);

			UnorderedDistributionInterceptor distributionInterceptor = new UnorderedDistributionInterceptor();
			registry.registerComponent(distributionInterceptor, UnorderedDistributionInterceptor.class);
			if (!chain.addInterceptorBefore(distributionInterceptor, NonTxDistributionInterceptor.class) &&
					!chain.addInterceptorBefore( distributionInterceptor, TriangleDistributionInterceptor.class)) {
				throw new IllegalStateException("Misconfigured cache, interceptor chain is " + chain);
			}

			chain.removeInterceptor(NonTxDistributionInterceptor.class);
			chain.removeInterceptor(TriangleDistributionInterceptor.class);
		}

		// ClusteredExpirationManager sends RemoteExpirationCommands to remote nodes which causes
		// undesired overhead. When get() triggers a RemoteExpirationCommand executed in async executor
		// this locks the entry for the duration of RPC, and putFromLoad with ZERO_LOCK_ACQUISITION_TIMEOUT
		// fails as it finds the entry being blocked.
		InternalExpirationManager expirationManager = registry.getComponent(InternalExpirationManager.class);
		if ((expirationManager instanceof ClusterExpirationManager)) {
			// re-registering component does not stop the old one
			((ClusterExpirationManager) expirationManager).stop();
			registry.registerComponent(new ExpirationManagerImpl<>(), InternalExpirationManager.class);
			registry.rewire();
		}
		else if (expirationManager instanceof ExpirationManagerImpl) {
			// do nothing
		}
		else {
			throw new IllegalStateException("Expected clustered expiration manager, found " + expirationManager);
		}

		registry.registerComponent(this, InfinispanDataRegion.class);

		if (cacheMode.isClustered()) {
			UnorderedReplicationLogic replLogic = new UnorderedReplicationLogic();
			registry.registerComponent( replLogic, ClusteringDependentLogic.class );
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
	protected void runInvalidation(boolean inTransaction) {
		if (strategy == null) {
			throw new IllegalStateException("Strategy was not set");
		}
		switch (strategy) {
			case NONE:
			case VALIDATION:
				super.runInvalidation(inTransaction);
				return;
			case TOMBSTONES:
				removeEntries(inTransaction, entry -> localCache.remove(entry.getKey(), entry.getValue()));
				return;
			case VERSIONED_ENTRIES:
				// no need to use this as a function - simply override all
				VersionedEntry evict = new VersionedEntry(nextTimestamp());
				removeEntries(inTransaction, entry ->
						localCache.put(entry.getKey(), evict, getTombstoneExpiration(), TimeUnit.MILLISECONDS));
				return;
		}
	}

	private void removeEntries(boolean inTransaction, Consumer<Map.Entry<Object, Object>> remover) {
		// If the transaction is required, we simply need it -> will create our own
		boolean startedTx = false;
		if ( !inTransaction && requiresTransaction) {
			try {
				tm.begin();
				startedTx = true;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		// We can never use cache.clear() since tombstones must be kept.
		try {
			localCache.entrySet().stream().filter(filter).forEach(remover);
		} finally {
			if (startedTx) {
				try {
					tm.commit();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	@Override
	public Comparator<Object> getComparator(String subclass) {
		return metadata.getVersionComparator();
	}

   @Override
   public void destroy() throws CacheException {
      super.destroy();
      if (validator != null) {
         validator.destroy();
      }
   }

}
