/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.entity;

import org.hibernate.cache.CacheException;
import org.infinispan.hibernate.cache.access.AccessDelegate;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.access.EntityRegionAccessStrategy;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.persister.entity.EntityPersister;

/**
 * A specialization of {@link ReadWriteAccess} that ensures we never update data.
 *
 * @author Chris Bredesen
 * @author Galder Zamarreño
 * @since 3.5
 */
class ReadOnlyAccess implements EntityRegionAccessStrategy {

	protected final EntityRegionImpl region;
	protected final AccessDelegate delegate;

	ReadOnlyAccess(EntityRegionImpl region, AccessDelegate delegate) {
		this.region = region;
		this.delegate = delegate;
	}

	public void evict(Object key) throws CacheException {
		delegate.evict( key );
	}

	public void evictAll() throws CacheException {
		delegate.evictAll();
	}

	public Object get(SessionImplementor session, Object key, long txTimestamp) throws CacheException {
		return delegate.get( session, key, txTimestamp );
	}

	public EntityRegion getRegion() {
		return this.region;
	}

	public boolean putFromLoad(SessionImplementor session, Object key, Object value, long txTimestamp, Object version) throws CacheException {
		return delegate.putFromLoad( session, key, value, txTimestamp, version );
	}

	public boolean putFromLoad(SessionImplementor session, Object key, Object value, long txTimestamp, Object version, boolean minimalPutOverride)
			throws CacheException {
		return delegate.putFromLoad( session, key, value, txTimestamp, version, minimalPutOverride );
	}

	public void remove(SessionImplementor session, Object key) throws CacheException {
		delegate.remove ( session, key );
	}

	public void removeAll() throws CacheException {
		delegate.removeAll();
	}

	public boolean insert(SessionImplementor session, Object key, Object value, Object version) throws CacheException {
		return delegate.insert( session, key, value, version );
	}

	@Override
	public boolean update(
			SessionImplementor session, Object key, Object value, Object currentVersion,
			Object previousVersion) throws CacheException {
		throw new UnsupportedOperationException( "Illegal attempt to edit read only item" );
	}

	public SoftLock lockItem(SessionImplementor session, Object key, Object version) throws CacheException {
		return null;
	}

	public SoftLock lockRegion() throws CacheException {
		return null;
	}

	public void unlockItem(SessionImplementor session, Object key, SoftLock lock) throws CacheException {
		delegate.unlockItem( session, key );
	}

	public void unlockRegion(SoftLock lock) throws CacheException {
	}

	public boolean afterInsert(SessionImplementor session, Object key, Object value, Object version) throws CacheException {
		return delegate.afterInsert( session, key, value, version );
	}

	@Override
	public boolean afterUpdate(
			SessionImplementor session, Object key, Object value, Object currentVersion,
			Object previousVersion, SoftLock lock) throws CacheException {
		throw new UnsupportedOperationException( "Illegal attempt to edit read only item" );
	}

	@Override
	public Object generateCacheKey(Object id, EntityPersister persister, SessionFactoryImplementor factory, String tenantIdentifier) {
		return region.getCacheKeysFactory().createEntityKey(id, persister, factory, tenantIdentifier);
	}

	@Override
	public Object getCacheKeyId(Object cacheKey) {
		return region.getCacheKeysFactory().getEntityId(cacheKey);
	}
}
