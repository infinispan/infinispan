/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.access;

import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;

import javax.transaction.Status;

/**
 * Synchronization that should release the locks after invalidation is complete.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class InvalidationSynchronization implements javax.transaction.Synchronization {
   private final static InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(InvalidationSynchronization.class);
	public final Object lockOwner;
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
      log.tracef("After completion callback with status %d", status);
		nonTxPutFromLoadInterceptor.endInvalidating(key, lockOwner, status == Status.STATUS_COMMITTED || status == Status.STATUS_COMMITTING);
	}
}
