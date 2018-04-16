/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.access;

import java.util.concurrent.CompletableFuture;

import org.hibernate.cache.CacheException;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.Param;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.hibernate.cache.commons.access.SessionAccess.TransactionCoordinatorAccess;
import org.infinispan.hibernate.cache.commons.InfinispanDataRegion;
import org.infinispan.hibernate.cache.commons.util.Caches;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.hibernate.cache.commons.util.Tombstone;
import org.infinispan.hibernate.cache.commons.util.TombstoneUpdate;
import org.hibernate.cache.spi.access.SoftLock;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class TombstoneAccessDelegate implements AccessDelegate {
	protected static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog( TombstoneAccessDelegate.class );
   private static final SessionAccess SESSION_ACCESS = SessionAccess.findSessionAccess();

	protected final InfinispanDataRegion region;
	protected final AdvancedCache cache;
	protected final FunctionalMap.ReadWriteMap<Object, Object> writeMap;
	protected final FunctionalMap.ReadWriteMap<Object, Object> asyncWriteMap;
	private final boolean requiresTransaction;
	private final FunctionalMap.ReadWriteMap<Object, Object> putFromLoadMap;

	public TombstoneAccessDelegate(InfinispanDataRegion region) {
		this.region = region;
		this.cache = region.getCache();
		FunctionalMapImpl<Object, Object> fmap = FunctionalMapImpl.create(cache).withParams(Param.PersistenceMode.SKIP_LOAD);
		writeMap = ReadWriteMapImpl.create(fmap);
		// Note that correct behaviour of local and async writes depends on LockingInterceptor (see there for details)
		asyncWriteMap = ReadWriteMapImpl.create(fmap).withParams(Param.ReplicationMode.ASYNC);
		putFromLoadMap = ReadWriteMapImpl.create(fmap).withParams(Param.LockingMode.TRY_LOCK, Param.ReplicationMode.ASYNC);
		Configuration configuration = this.cache.getCacheConfiguration();
		if (configuration.clustering().cacheMode().isInvalidation()) {
			throw new IllegalArgumentException("For tombstone-based caching, invalidation cache is not allowed.");
		}
		if (configuration.transaction().transactionMode().isTransactional()) {
			throw new IllegalArgumentException("Currently transactional caches are not supported.");
		}
		requiresTransaction = configuration.transaction().transactionMode().isTransactional()
				&& !configuration.transaction().autoCommit();
	}

	@Override
	public Object get(Object session, Object key, long txTimestamp) throws CacheException {
		if (txTimestamp < region.getLastRegionInvalidation() ) {
			return null;
		}
		Object value = cache.get(key);
		if (value instanceof Tombstone) {
			return null;
		} else {
			return value;
		}
	}

	@Override
	public boolean putFromLoad(Object session, Object key, Object value, long txTimestamp, Object version) {
		return putFromLoad(session, key, value, txTimestamp, version, false);
	}

	@Override
	public boolean putFromLoad(Object session, Object key, Object value, long txTimestamp, Object version, boolean minimalPutOverride) throws CacheException {
		long lastRegionInvalidation = region.getLastRegionInvalidation();
		if (txTimestamp < lastRegionInvalidation) {
			log.tracef("putFromLoad not executed since tx started at %d, before last region invalidation finished = %d", txTimestamp, lastRegionInvalidation);
			return false;
		}
		if (minimalPutOverride) {
			Object prev = cache.get(key);
			if (prev instanceof Tombstone) {
				Tombstone tombstone = (Tombstone) prev;
				long lastTimestamp = tombstone.getLastTimestamp();
				if (txTimestamp <= lastTimestamp) {
					log.tracef("putFromLoad not executed since tx started at %d, before last invalidation finished = %d", txTimestamp, lastTimestamp);
					return false;
				}
			}
			else if (prev != null) {
				log.tracef("putFromLoad not executed since cache contains %s", prev);
				return false;
			}
		}
		// we can't use putForExternalRead since the PFER flag means that entry is not wrapped into context
		// when it is present in the container. TombstoneCallInterceptor will deal with this.
		CompletableFuture<Void> future = putFromLoadMap.eval(key, new TombstoneUpdate<>(SESSION_ACCESS.getTimestamp(session), value));
		assert future.isDone(); // async try-locking should be done immediately
		return true;
	}

	@Override
	public boolean insert(Object session, Object key, Object value, Object version) throws CacheException {
		write(session, key, value);
		return true;
	}

	@Override
	public boolean update(Object session, Object key, Object value, Object currentVersion, Object previousVersion) throws CacheException {
		write(session, key, value);
		return true;
	}

	@Override
	public void remove(Object session, Object key) throws CacheException {
		write(session, key, null);
	}

	protected void write(Object session, Object key, Object value) {
      TransactionCoordinatorAccess tc = SESSION_ACCESS.getTransactionCoordinator(session);
      long timestamp = SESSION_ACCESS.getTimestamp(session);
      FutureUpdateSynchronization sync = new FutureUpdateSynchronization(tc, asyncWriteMap, requiresTransaction, key, value, region, timestamp);
		// The update will be invalidating all putFromLoads for the duration of expiration or until removed by the synchronization
		Tombstone tombstone = new Tombstone(sync.getUuid(), region.nextTimestamp() + region.getTombstoneExpiration());
		// The outcome of this operation is actually defined in TombstoneCallInterceptor
		// Metadata in PKVC are cleared and set in the interceptor, too
		writeMap.eval(key, tombstone).join();
      tc.registerLocalSynchronization(sync);
	}

	@Override
	public void removeAll() throws CacheException {
		region.beginInvalidation();
		try {
			Caches.broadcastEvictAll(cache);
		}
		finally {
			region.endInvalidation();
		}
	}

	@Override
	public void evict(Object key) throws CacheException {
		writeMap.eval(key, new TombstoneUpdate<>(region.nextTimestamp(), null)).join();
	}

	@Override
	public void evictAll() throws CacheException {
		region.beginInvalidation();
		try {
			Caches.broadcastEvictAll(cache);
		}
		finally {
			region.endInvalidation();
		}
	}

	@Override
	public void unlockItem(Object session, Object key) throws CacheException {
	}

	@Override
	public boolean afterInsert(Object session, Object key, Object value, Object version) {
		return false;
	}

	@Override
	public boolean afterUpdate(Object session, Object key, Object value, Object currentVersion, Object previousVersion, SoftLock lock) {
		return false;
	}
}
