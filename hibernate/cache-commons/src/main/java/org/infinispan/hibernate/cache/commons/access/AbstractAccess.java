/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.access;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.SoftLock;

/**
 * Collection region access for Infinispan.
 *
 * @author Chris Bredesen
 * @author Galder Zamarreño
 * @since 3.5
 */
public abstract class AbstractAccess {

	protected final AccessDelegate delegate;

	public AbstractAccess(AccessDelegate delegate) {
		this.delegate = delegate;
	}

	public void evict(Object key) throws CacheException {
		delegate.evict( key );
	}

	public void evictAll() throws CacheException {
		delegate.evictAll();
	}

	public void removeAll() throws CacheException {
		delegate.removeAll();
	}

	public SoftLock lockRegion() throws CacheException {
		return null;
	}

	public void unlockRegion(SoftLock lock) throws CacheException {
	}

}
