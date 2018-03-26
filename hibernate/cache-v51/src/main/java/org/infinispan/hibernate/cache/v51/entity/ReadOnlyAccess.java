package org.infinispan.hibernate.cache.v51.entity;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.access.EntityRegionAccessStrategy;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;
import org.infinispan.hibernate.cache.v51.access.AbstractAccess;

class ReadOnlyAccess extends AbstractAccess implements EntityRegionAccessStrategy {

   private final EntityRegionImpl region;

   public ReadOnlyAccess(EntityRegionImpl region, AccessDelegate delegate) {
      super(delegate);
      this.region = region;
   }

   @Override
   public boolean insert(SessionImplementor session, Object key, Object value, Object version) throws CacheException {
      return delegate.insert(session, key, value, version);
   }

   @Override
   public boolean afterInsert(SessionImplementor session, Object key, Object value, Object version) throws CacheException {
      return delegate.afterInsert(session, key, value, version);
   }

   @Override
   public boolean update(SessionImplementor session, Object key, Object value, Object currentVersion, Object previousVersion) throws CacheException {
      throw new UnsupportedOperationException("Illegal attempt to edit read only item");
   }

   @Override
   public boolean afterUpdate(SessionImplementor session, Object key, Object value, Object currentVersion, Object previousVersion, SoftLock lock) throws CacheException {
      throw new UnsupportedOperationException("Illegal attempt to edit read only item");
   }

   @Override
   public Object get(SessionImplementor session, Object key, long txTimestamp) throws CacheException {
      return delegate.get(session, key, txTimestamp);
   }

   @Override
   public boolean putFromLoad(SessionImplementor session, Object key, Object value, long txTimestamp, Object version) throws CacheException {
      return delegate.putFromLoad(session, key, value, txTimestamp, version);
   }

   @Override
   public boolean putFromLoad(SessionImplementor session, Object key, Object value, long txTimestamp, Object version, boolean minimalPutOverride) throws CacheException {
      return delegate.putFromLoad(session, key, value, txTimestamp, version, minimalPutOverride);
   }

   @Override
   public SoftLock lockItem(SessionImplementor session, Object key, Object version) throws CacheException {
      return null;
   }

   @Override
   public void unlockItem(SessionImplementor session, Object key, SoftLock lock) throws CacheException {
      delegate.unlockItem(session, key);
   }

   @Override
   public void remove(SessionImplementor session, Object key) throws CacheException {
      delegate.remove (session, key);
   }

   @Override
   public Object generateCacheKey(Object id, EntityPersister persister, SessionFactoryImplementor factory, String tenantIdentifier) {
      return region.getCacheKeysFactory().createEntityKey(id, persister, factory, tenantIdentifier);
   }

   @Override
   public Object getCacheKeyId(Object cacheKey) {
      return region.getCacheKeysFactory().getEntityId(cacheKey);
   }

   @Override
   public EntityRegion getRegion() {
      return region;
   }

}
