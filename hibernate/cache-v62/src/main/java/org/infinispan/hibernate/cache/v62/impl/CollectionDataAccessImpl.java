package org.infinispan.hibernate.cache.v62.impl;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.collection.CollectionPersister;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;

public class CollectionDataAccessImpl extends AbstractAccess implements CollectionDataAccess {

   public CollectionDataAccessImpl(DomainDataRegionImpl region, AccessDelegate delegate, AccessType accessType) {
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
   public void remove(SharedSessionContractImplementor session, Object key) throws CacheException {
      delegate.remove(session, key);
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
   public Object generateCacheKey(Object id, CollectionPersister persister, SessionFactoryImplementor factory, String tenantIdentifier) {
      return region.getCacheKeysFactory().createCollectionKey(id, persister, factory, tenantIdentifier);
   }

   @Override
   public Object getCacheKeyId(Object cacheKey) {
      return region.getCacheKeysFactory().getCollectionId(cacheKey);
   }

}
