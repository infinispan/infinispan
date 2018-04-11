/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.access;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.hibernate.cache.commons.access.SessionAccess.TransactionCoordinatorAccess;
import org.infinispan.hibernate.cache.commons.InfinispanDataRegion;
import org.infinispan.hibernate.cache.commons.util.InvocationAfterCompletion;
import org.infinispan.hibernate.cache.commons.util.VersionedEntry;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class RemovalSynchronization extends InvocationAfterCompletion {
	private final InfinispanDataRegion region;
	private final Object key;
	private final FunctionalMap.ReadWriteMap<Object, Object> rwMap;

	public RemovalSynchronization(TransactionCoordinatorAccess tc, FunctionalMap.ReadWriteMap<Object, Object> rwMap, boolean requiresTransaction, InfinispanDataRegion region, Object key) {
		super(tc, requiresTransaction);
		this.rwMap = rwMap;
		this.region = region;
		this.key = key;
	}

	@Override
	protected void invoke(boolean success) {
		if (success) {
			rwMap.eval(key, new VersionedEntry(region.nextTimestamp())).join();
		}
	}
}
