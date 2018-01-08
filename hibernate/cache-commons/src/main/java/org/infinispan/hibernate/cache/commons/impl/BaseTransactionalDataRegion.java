/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.impl;

import org.infinispan.hibernate.cache.commons.InfinispanRegionFactory;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;
import org.infinispan.hibernate.cache.commons.access.LockingInterceptor;
import org.infinispan.hibernate.cache.commons.access.NonStrictAccessDelegate;
import org.infinispan.hibernate.cache.commons.access.NonTxInvalidationCacheAccessDelegate;
import org.infinispan.hibernate.cache.commons.access.PutFromLoadValidator;
import org.infinispan.hibernate.cache.commons.access.TombstoneAccessDelegate;
import org.infinispan.hibernate.cache.commons.access.TombstoneCallInterceptor;
import org.infinispan.hibernate.cache.commons.access.TxInvalidationCacheAccessDelegate;
import org.infinispan.hibernate.cache.commons.access.UnorderedDistributionInterceptor;
import org.infinispan.hibernate.cache.commons.access.UnorderedReplicationLogic;
import org.infinispan.hibernate.cache.commons.access.VersionedCallInterceptor;
import org.infinispan.hibernate.cache.commons.util.Caches;
import org.infinispan.hibernate.cache.commons.util.FutureUpdate;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.hibernate.cache.commons.util.Tombstone;
import org.infinispan.hibernate.cache.commons.util.VersionedEntry;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.cache.spi.TransactionalDataRegion;

import org.hibernate.cache.spi.access.AccessType;
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
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.distribution.NonTxDistributionInterceptor;
import org.infinispan.interceptors.distribution.TriangleDistributionInterceptor;
import org.infinispan.interceptors.impl.CallInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;

import javax.transaction.TransactionManager;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Support for Inifinispan {@link org.hibernate.cache.spi.TransactionalDataRegion} implementors.
 *
 * @author Chris Bredesen
 * @author Galder Zamarreño
 * @since 3.5
 */
public abstract class BaseTransactionalDataRegion
		extends BaseRegion implements TransactionalDataRegion {
	private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog( BaseTransactionalDataRegion.class );
	private final CacheDataDescription metadata;
	private final CacheKeysFactory cacheKeysFactory;
	private final boolean requiresTransaction;

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

		Configuration configuration = cache.getCacheConfiguration();
		requiresTransaction = configuration.transaction().transactionMode().isTransactional()
				&& !configuration.transaction().autoCommit();
		tombstoneExpiration = factory.getPendingPutsCacheConfiguration().expiration().maxIdle();
		if (!isRegionAccessStrategyEnabled()) {
			strategy = Strategy.NONE;
		}
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
			return new NonStrictAccessDelegate(this);
		}
		if (cacheMode.isDistributed() || cacheMode.isReplicated()) {
			prepareForTombstones();
			return new TombstoneAccessDelegate(this);
		}
		else {
			prepareForValidation();
			if (cache.getCacheConfiguration().transaction().transactionMode().isTransactional()) {
				return new TxInvalidationCacheAccessDelegate(this, validator);
			}
			else {
				return new NonTxInvalidationCacheAccessDelegate(this, validator);
			}
		}
	}

	protected void prepareForValidation() {
		if (strategy != null) {
			assert strategy == Strategy.VALIDATION;
			return;
		}
		validator = new PutFromLoadValidator(cache, factory);
		strategy = Strategy.VALIDATION;
	}

	protected void prepareForVersionedEntries(CacheMode cacheMode) {
		if (strategy != null) {
			assert strategy == Strategy.VERSIONED_ENTRIES;
			return;
		}

		replaceCommonInterceptors();
		replaceExpirationManager();

      VersionedCallInterceptor versionedCallInterceptor = new VersionedCallInterceptor(this, metadata.getVersionComparator());
      ComponentRegistry compReg = cache.getComponentRegistry();
      compReg.registerComponent(versionedCallInterceptor, VersionedCallInterceptor.class);
      AsyncInterceptorChain interceptorChain = cache.getAsyncInterceptorChain();
      interceptorChain.addInterceptorBefore(versionedCallInterceptor, CallInterceptor.class);

      if (cacheMode.isClustered()) {
         UnorderedReplicationLogic replLogic = new UnorderedReplicationLogic();
         compReg.registerComponent( replLogic, ClusteringDependentLogic.class );
         compReg.rewire();
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

	public long getLastRegionInvalidation() {
		return lastRegionInvalidation;
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
				removeEntries(inTransaction, Tombstone.EXCLUDE_TOMBSTONES);
				return;
			case VERSIONED_ENTRIES:
				removeEntries(inTransaction, VersionedEntry.EXCLUDE_EMPTY_EXTRACT_VALUE);
				return;
		}
	}

	private void removeEntries(boolean inTransaction, KeyValueFilter filter) {
		// If the transaction is required, we simply need it -> will create our own
		boolean startedTx = false;
		if ( !inTransaction && requiresTransaction) {
			try {
				tm.begin();
				startedTx = true;
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		// We can never use cache.clear() since tombstones must be kept.
		try {
			AdvancedCache localCache = Caches.localCache(cache);
			CloseableIterator<CacheEntry> it = Caches.entrySet(localCache, Tombstone.EXCLUDE_TOMBSTONES).iterator();
			long now = nextTimestamp();
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
		finally {
			if (startedTx) {
				try {
					tm.commit();
				}
				catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	@Override
	public Map toMap() {
		if (strategy == null) {
			throw new IllegalStateException("Strategy was not set");
		}
		switch (strategy) {
			case NONE:
			case VALIDATION:
				return super.toMap();
			case TOMBSTONES:
				return Caches.entrySet(Caches.localCache(cache), Tombstone.EXCLUDE_TOMBSTONES).toMap();
			case VERSIONED_ENTRIES:
				return Caches.entrySet(Caches.localCache(cache), VersionedEntry.EXCLUDE_EMPTY_EXTRACT_VALUE, VersionedEntry.EXCLUDE_EMPTY_EXTRACT_VALUE).toMap();
			default:
				throw new IllegalStateException(strategy.toString());
		}
	}

	@Override
	public boolean contains(Object key) {
		if (!checkValid()) {
			return false;
		}
		Object value = cache.get(key);
		if (value instanceof Tombstone) {
			return false;
		}
		if (value instanceof FutureUpdate) {
			return ((FutureUpdate) value).getValue() != null;
		}
		if (value instanceof VersionedEntry) {
			return ((VersionedEntry) value).getValue() != null;
		}
		return value != null;
	}

   @Override
   public void destroy() throws CacheException {
      super.destroy();
      if (validator != null) {
         validator.destroy();
      }
   }

}
