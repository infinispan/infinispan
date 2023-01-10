package org.infinispan.hibernate.cache.v62.impl;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;

class ReadOnlyNaturalDataAccess extends AbstractAccess implements NaturalIdDataAccess {
   ReadOnlyNaturalDataAccess(AccessType accessType, AccessDelegate delegate, DomainDataRegionImpl region) {
      super(accessType, delegate, region);
   }

   @Override
   public boolean insert(SharedSessionContractImplementor session, Object key, Object value) throws CacheException {
      return delegate.insert(session, key, value, null);
   }

   @Override
   public boolean afterInsert(SharedSessionContractImplementor session, Object key, Object value) throws CacheException {
      return delegate.afterInsert(session, key, value, null);
   }

   @Override
   public boolean update(SharedSessionContractImplementor session, Object key, Object value) throws CacheException {
      throw new UnsupportedOperationException("Illegal attempt to edit read only item");
   }

   @Override
   public boolean afterUpdate(SharedSessionContractImplementor session, Object key, Object value, SoftLock lock) throws CacheException {
      return delegate.afterUpdate(session, key, value, null, null, lock);
   }

   @Override
   public Object get(SharedSessionContractImplementor session, Object key) throws CacheException {
      return delegate.get(session, key, session.getCacheTransactionSynchronization().getCachingTimestamp());
   }

   @Override
   public boolean putFromLoad(SharedSessionContractImplementor session, Object key, Object value, Object version) throws CacheException {
      return delegate.putFromLoad(session, key, value, session.getCacheTransactionSynchronization().getCachingTimestamp(), version);
   }

   @Override
   public boolean putFromLoad(SharedSessionContractImplementor session, Object key, Object value, Object version, boolean minimalPutOverride) throws CacheException {
      return delegate.putFromLoad(session, key, value, session.getCacheTransactionSynchronization().getCachingTimestamp(), version, minimalPutOverride);
   }

   @Override
   public SoftLock lockItem(SharedSessionContractImplementor session, Object key, Object version) throws CacheException {
      return null;
   }

   @Override
   public void unlockItem(SharedSessionContractImplementor session, Object key, SoftLock lock) throws CacheException {
      delegate.unlockItem(session, key);
   }

   @Override
   public void remove(SharedSessionContractImplementor session, Object key) throws CacheException {
      delegate.remove(session, key);
   }

   @Override
   public Object generateCacheKey(Object naturalIdValues, EntityPersister persister, SharedSessionContractImplementor session) {
      return region.getCacheKeysFactory().createNaturalIdKey(naturalIdValues, persister, session);
   }

   @Override
   public Object getNaturalIdValues(Object cacheKey) {
      return region.getCacheKeysFactory().getNaturalIdValues(cacheKey);
   }

}
