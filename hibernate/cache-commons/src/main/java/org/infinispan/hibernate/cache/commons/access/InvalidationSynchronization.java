package org.infinispan.hibernate.cache.commons.access;

import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;

import jakarta.transaction.Status;

/**
 * Synchronization that should release the locks after invalidation is complete.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class InvalidationSynchronization implements jakarta.transaction.Synchronization {
	private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(InvalidationSynchronization.class);

	private final Object lockOwner;
	private final NonTxPutFromLoadInterceptor nonTxPutFromLoadInterceptor;
	private final Object key;

	public InvalidationSynchronization(NonTxPutFromLoadInterceptor nonTxPutFromLoadInterceptor, Object key, Object lockOwner) {
		assert lockOwner != null;
		this.nonTxPutFromLoadInterceptor = nonTxPutFromLoadInterceptor;
		this.key = key;
		this.lockOwner = lockOwner;
	}

	@Override
	public void beforeCompletion() {}

	@Override
	public void afterCompletion(int status) {
		if (log.isTraceEnabled()) {
			log.tracef("After completion callback with status %d", status);
		}
		nonTxPutFromLoadInterceptor.endInvalidating(key, lockOwner, status == Status.STATUS_COMMITTED || status == Status.STATUS_COMMITTING);
	}
}
