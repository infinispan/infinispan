/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.access;

import java.util.UUID;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.hibernate.cache.commons.access.SessionAccess.TransactionCoordinatorAccess;
import org.infinispan.hibernate.cache.commons.InfinispanDataRegion;
import org.infinispan.hibernate.cache.commons.util.FutureUpdate;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.hibernate.cache.commons.util.InvocationAfterCompletion;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class FutureUpdateSynchronization extends InvocationAfterCompletion {
	private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog( FutureUpdateSynchronization.class );

	private final UUID uuid = UUID.randomUUID();
	private final Object key;
	private final Object value;
	private final InfinispanDataRegion region;
	private final long sessionTimestamp;
	private final FunctionalMap.ReadWriteMap<Object, Object> rwMap;

	public FutureUpdateSynchronization(TransactionCoordinatorAccess tc, FunctionalMap.ReadWriteMap<Object, Object> rwMap, boolean requiresTransaction,
												  Object key, Object value, InfinispanDataRegion region, long sessionTimestamp) {

		super(tc, requiresTransaction);
		this.rwMap = rwMap;
		this.key = key;
		this.value = value;
		this.region = region;
		this.sessionTimestamp = sessionTimestamp;
	}

	public UUID getUuid() {
		return uuid;
	}

	@Override
	protected void invoke(boolean success) {
		// If the region was invalidated during this session, we can't know that the value we're inserting is valid
		// so we'll just null the tombstone
		if (sessionTimestamp < region.getLastRegionInvalidation()) {
			success = false;
		}
		// Exceptions in #afterCompletion() are silently ignored, since the transaction
		// is already committed in DB. However we must not return until we update the cache.
		FutureUpdate futureUpdate = new FutureUpdate(uuid, region.nextTimestamp(), success ? this.value : null);
		for (;;) {
			try {
				// We expect that when the transaction completes further reads from cache will return the updated value.
				// UnorderedDistributionInterceptor makes sure that the update is executed on the node first, and here
				// we're waiting for the local update. The remote update does not concern us - the cache is async and
				// we won't wait for that.
				rwMap.eval(key, futureUpdate).join();
				return;
			}
			catch (Exception e) {
				log.failureInAfterCompletion(e);
			}
		}
	}
}
