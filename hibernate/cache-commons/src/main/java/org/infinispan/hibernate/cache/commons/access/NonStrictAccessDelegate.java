/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.access;

import java.util.Comparator;
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
import org.infinispan.hibernate.cache.commons.util.VersionedEntry;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.cache.spi.entry.CacheEntry;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;

/**
 * Access delegate that relaxes the consistency a bit: stale reads are prohibited only after the transaction
 * commits. This should also be able to work with async caches, and that would allow the replication delay
 * even after the commit.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class NonStrictAccessDelegate implements AccessDelegate {
	private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog( NonStrictAccessDelegate.class );
	private static final boolean trace = log.isTraceEnabled();
   private static final SessionAccess SESSION_ACCESS = SessionAccess.findSessionAccess();

	private final InfinispanDataRegion region;
	private final AdvancedCache cache;
	private final FunctionalMap.ReadWriteMap<Object, Object> writeMap;
	private final FunctionalMap.ReadWriteMap<Object, Object> putFromLoadMap;
	private final Comparator versionComparator;


	public NonStrictAccessDelegate(InfinispanDataRegion region, Comparator versionComparator) {
		this.region = region;
		this.cache = region.getCache();
		FunctionalMapImpl fmap = FunctionalMapImpl.create(cache).withParams(Param.PersistenceMode.SKIP_LOAD);
		this.writeMap = ReadWriteMapImpl.create(fmap);
		// Note that correct behaviour of local and async writes depends on LockingInterceptor (see there for details)
		this.putFromLoadMap = ReadWriteMapImpl.create(fmap).withParams(Param.LockingMode.TRY_LOCK, Param.ReplicationMode.ASYNC);
		Configuration configuration = cache.getCacheConfiguration();
		if (configuration.clustering().cacheMode().isInvalidation()) {
			throw new IllegalArgumentException("Nonstrict-read-write mode cannot use invalidation.");
		}
		if (configuration.transaction().transactionMode().isTransactional()) {
			throw new IllegalArgumentException("Currently transactional caches are not supported.");
		}
		this.versionComparator = versionComparator;
		if (versionComparator == null) {
			throw new IllegalArgumentException("This strategy requires versioned entities/collections but region " + region.getName() + " contains non-versioned data!");
		}
	}

	@Override
	public Object get(Object session, Object key, long txTimestamp) throws CacheException {
		if (txTimestamp < region.getLastRegionInvalidation() ) {
			return null;
		}
		Object value = cache.get(key);
		if (value instanceof VersionedEntry) {
			return ((VersionedEntry) value).getValue();
		}
		return value;
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
		assert version != null;

		if (minimalPutOverride) {
			Object prev = cache.get(key);
			if (prev != null) {
				Object oldVersion = getVersion(prev);
				if (oldVersion != null) {
					if (versionComparator.compare(version, oldVersion) <= 0) {
						if (trace) {
							log.tracef("putFromLoad not executed since version(%s) <= oldVersion(%s)", version, oldVersion);
						}
						return false;
					}
				}
				else if (prev instanceof VersionedEntry && txTimestamp <= ((VersionedEntry) prev).getTimestamp()) {
					if (trace) {
						log.tracef("putFromLoad not executed since tx started at %d and entry was invalidated at %d",
								txTimestamp, ((VersionedEntry) prev).getTimestamp());
					}
					return false;
				}
			}
		}
		// we can't use putForExternalRead since the PFER flag means that entry is not wrapped into context
		// when it is present in the container. TombstoneCallInterceptor will deal with this.
		// Even if value is instanceof CacheEntry, we have to wrap it in VersionedEntry and add transaction timestamp.
		// Otherwise, old eviction record wouldn't be overwritten.
		CompletableFuture<Void> future = putFromLoadMap.eval(key, new VersionedEntry(value, version, txTimestamp));
		assert future.isDone(); // async try-locking should be done immediately
		return true;
	}

	@Override
	public boolean insert(Object session, Object key, Object value, Object version) throws CacheException {
		return false;
	}

	@Override
	public boolean update(Object session, Object key, Object value, Object currentVersion, Object previousVersion) throws CacheException {
		return false;
	}

	@Override
	public void remove(Object session, Object key) throws CacheException {
		// there's no 'afterRemove', so we have to use our own synchronization
		// the API does not provide version of removed item but we can't load it from the cache
		// as that would be prone to race conditions - if the entry was updated in the meantime
		// the remove could be discarded and we would end up with stale record
		// See VersionedTest#testCollectionUpdate for such situation
      TransactionCoordinatorAccess transactionCoordinator = SESSION_ACCESS.getTransactionCoordinator(session);
		RemovalSynchronization sync = new RemovalSynchronization(transactionCoordinator, writeMap, false, region, key);
		transactionCoordinator.registerLocalSynchronization(sync);
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
		writeMap.eval(key, new VersionedEntry(region.nextTimestamp())).join();
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
		assert value != null;
		assert version != null;
		writeMap.eval(key, new VersionedEntry(value, version, SESSION_ACCESS.getTimestamp(session))).join();
		return true;
	}

	@Override
	public boolean afterUpdate(Object session, Object key, Object value, Object currentVersion, Object previousVersion, SoftLock lock) {
		assert value != null;
		assert currentVersion != null;
		writeMap.eval(key, new VersionedEntry(value, currentVersion, SESSION_ACCESS.getTimestamp(session))).join();
		return true;
	}

	protected Object getVersion(Object value) {
		if (value instanceof CacheEntry) {
			return ((CacheEntry) value).getVersion();
		}
		else if (value instanceof VersionedEntry) {
			return ((VersionedEntry) value).getVersion();
		}
		return null;
	}

}
