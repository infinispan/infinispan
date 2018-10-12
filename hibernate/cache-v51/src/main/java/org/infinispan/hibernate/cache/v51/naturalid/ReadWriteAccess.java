/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.v51.naturalid;

import org.hibernate.cache.CacheException;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SessionImplementor;

/**
 * @author Strong Liu &lt;stliu@hibernate.org&gt;
 */
class ReadWriteAccess extends ReadOnlyAccess {

	ReadWriteAccess(NaturalIdRegionImpl region, AccessDelegate delegate) {
		super(region, delegate);
	}

	@Override
	public boolean update(SessionImplementor session, Object key, Object value) throws CacheException {
		return delegate.update( session, key, value, null, null );
	}

	@Override
	public boolean afterUpdate(SessionImplementor session, Object key, Object value, SoftLock lock) throws CacheException {
		return delegate.afterUpdate( session, key, value, null, null, lock );
	}

}
