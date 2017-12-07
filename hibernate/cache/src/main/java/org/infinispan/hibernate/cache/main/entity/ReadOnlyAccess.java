package org.infinispan.hibernate.cache.main.entity;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.access.EntityRegionAccessStrategy;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;

class ReadOnlyAccess
   extends org.infinispan.hibernate.cache.commons.entity.ReadOnlyAccess
   implements EntityRegionAccessStrategy {

   private final EntityRegionImpl region;

   public ReadOnlyAccess(EntityRegionImpl region, AccessDelegate delegate) {
      super(delegate);
      this.region = region;
   }

   @Override
   public boolean insert(SharedSessionContractImplementor session, Object key, Object value, Object version) throws CacheException {
      return delegate.insert(session, key, value, version);
   }

   @Override
   public boolean afterInsert(SharedSessionContractImplementor session, Object key, Object value, Object version) throws CacheException {
      return delegate.afterInsert(session, key, value, version);
   }

   @Override
   public boolean update(SharedSessionContractImplementor session, Object key, Object value, Object currentVersion, Object previousVersion) throws CacheException {
      throw new UnsupportedOperationException( "Illegal attempt to edit read only item" );
   }

   @Override
   public boolean afterUpdate(SharedSessionContractImplementor session, Object o, Object o1, Object o2, Object o3, SoftLock softLock) throws CacheException {
      throw new UnsupportedOperationException("Illegal attempt to edit read only item");
   }

   @Override
   public Object get(SharedSessionContractImplementor session, Object key, long txTimestamp) throws CacheException {
      return delegate.get(session, key, txTimestamp);
   }

   @Override
   public boolean putFromLoad(SharedSessionContractImplementor session, Object key, Object value, long txTimestamp, Object version) throws CacheException {
      return delegate.putFromLoad( session, key, value, txTimestamp, version);
   }

   @Override
   public boolean putFromLoad(SharedSessionContractImplementor session, Object key, Object value, long txTimestamp, Object version, boolean minimalPutOverride) throws CacheException {
      return delegate.putFromLoad(session, key, value, txTimestamp, version, minimalPutOverride);
   }

   @Override
   public SoftLock lockItem(SharedSessionContractImplementor session, Object o, Object o1) throws CacheException {
      return null;
   }

   @Override
   public void unlockItem(SharedSessionContractImplementor session, Object key, SoftLock softLock) throws CacheException {
      delegate.unlockItem(session, key);
   }

   @Override
   public void remove(SharedSessionContractImplementor session, Object key) throws CacheException {
      delegate.remove (session, key);
   }

   public Object generateCacheKey(Object id, EntityPersister persister, SessionFactoryImplementor factory, String tenantIdentifier) {
      return region.getCacheKeysFactory().createEntityKey(id, persister, factory, tenantIdentifier);
   }

   public Object getCacheKeyId(Object cacheKey) {
      return region.getCacheKeysFactory().getEntityId(cacheKey);
   }

   @Override
   public EntityRegion getRegion() {
      return region;
   }

}
