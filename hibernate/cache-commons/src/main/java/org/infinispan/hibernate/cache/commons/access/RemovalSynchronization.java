/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.access;

import java.util.concurrent.TimeUnit;

import org.infinispan.hibernate.cache.commons.access.SessionAccess.TransactionCoordinatorAccess;
import org.infinispan.hibernate.cache.commons.InfinispanDataRegion;
import org.infinispan.hibernate.cache.commons.util.InvocationAfterCompletion;
import org.infinispan.hibernate.cache.commons.util.VersionedEntry;

import org.infinispan.AdvancedCache;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class RemovalSynchronization extends InvocationAfterCompletion {
	private final InfinispanDataRegion region;
	private final Object key;
	private final AdvancedCache cache;

	public RemovalSynchronization(TransactionCoordinatorAccess tc, AdvancedCache cache, boolean requiresTransaction, InfinispanDataRegion region, Object key) {
		super(tc, requiresTransaction);
		this.cache = cache;
		this.region = region;
		this.key = key;
	}

	@Override
	protected void invoke(boolean success) {
		if (success) {
			cache.put(key, new VersionedEntry(null, null, region.nextTimestamp()), region.getTombstoneExpiration(), TimeUnit.MILLISECONDS);
		}
	}
}
