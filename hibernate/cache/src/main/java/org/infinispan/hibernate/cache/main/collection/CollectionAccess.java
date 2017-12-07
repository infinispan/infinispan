package org.infinispan.hibernate.cache.main.collection;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CollectionRegion;
import org.hibernate.cache.spi.access.CollectionRegionAccessStrategy;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.collection.CollectionPersister;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;

public class CollectionAccess
   extends org.infinispan.hibernate.cache.commons.collection.CollectionAccess
   implements CollectionRegionAccessStrategy {

   private final CollectionRegionImpl region;

   public CollectionAccess(CollectionRegionImpl region, AccessDelegate delegate) {
      super(delegate);
      this.region = region;
   }

   @Override
   public Object get(SharedSessionContractImplementor session, Object key, long txTimestamp) throws CacheException {
      return delegate.get(session, key, txTimestamp);
   }

   @Override
   public boolean putFromLoad(SharedSessionContractImplementor session, Object key, Object value, long txTimestamp, Object version) throws CacheException {
      return delegate.putFromLoad(session, key, value, txTimestamp, version);
   }

   @Override
   public boolean putFromLoad(SharedSessionContractImplementor session, Object key, Object value, long txTimestamp, Object version, boolean minimalPutOverride) throws CacheException {
      return delegate.putFromLoad(session, key, value, txTimestamp, version, minimalPutOverride);
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
   public Object generateCacheKey(Object id, CollectionPersister persister, SessionFactoryImplementor factory, String tenantIdentifier) {
      return region.getCacheKeysFactory().createCollectionKey(id, persister, factory, tenantIdentifier);
   }

   @Override
   public Object getCacheKeyId(Object cacheKey) {
      return region.getCacheKeysFactory().getCollectionId(cacheKey);
   }

   @Override
   public CollectionRegion getRegion() {
      return region;
   }

}
