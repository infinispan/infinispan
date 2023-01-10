package org.infinispan.hibernate.cache.v62.impl;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;

class ReadOnlyEntityDataAccess extends AbstractAccess implements EntityDataAccess {

   ReadOnlyEntityDataAccess(AccessType accessType, AccessDelegate delegate, DomainDataRegionImpl region) {
      super(accessType, delegate, region);
   }

   @Override
   public Object get(SharedSessionContractImplementor session, Object key) {
      return delegate.get(session, key, session.getCacheTransactionSynchronization().getCachingTimestamp());
   }

   @Override
   public boolean putFromLoad(SharedSessionContractImplementor session, Object key, Object value, Object version) {
      return delegate.putFromLoad(session, key, value, session.getCacheTransactionSynchronization().getCachingTimestamp(), version);
   }

   @Override
   public boolean putFromLoad(SharedSessionContractImplementor session, Object key, Object value, Object version, boolean minimalPutOverride) {
      return delegate.putFromLoad(session, key, value, session.getCacheTransactionSynchronization().getCachingTimestamp(), version, minimalPutOverride);
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
      throw new UnsupportedOperationException("Illegal attempt to edit read only item");
   }

   @Override
   public boolean afterUpdate(SharedSessionContractImplementor session, Object o, Object o1, Object o2, Object o3, SoftLock softLock) throws CacheException {
      throw new UnsupportedOperationException("Illegal attempt to edit read only item");
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
      delegate.remove(session, key);
   }

   public Object generateCacheKey(Object id, EntityPersister persister, SessionFactoryImplementor factory, String tenantIdentifier) {
      return region.getCacheKeysFactory().createEntityKey(id, persister, factory, tenantIdentifier);
   }

   public Object getCacheKeyId(Object cacheKey) {
      return region.getCacheKeysFactory().getEntityId(cacheKey);
   }
}
