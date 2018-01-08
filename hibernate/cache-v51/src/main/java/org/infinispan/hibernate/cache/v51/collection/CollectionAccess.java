package org.infinispan.hibernate.cache.v51.collection;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CollectionRegion;
import org.hibernate.cache.spi.access.CollectionRegionAccessStrategy;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.persister.collection.CollectionPersister;
import org.infinispan.hibernate.cache.commons.access.AbstractAccess;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;

public class CollectionAccess
   extends AbstractAccess
   implements CollectionRegionAccessStrategy {

   private final CollectionRegionImpl region;

   public CollectionAccess(CollectionRegionImpl region, AccessDelegate delegate) {
      super(delegate);
      this.region = region;
   }

   public Object get(SessionImplementor session, Object key, long txTimestamp) throws CacheException {
      return delegate.get(session, key, txTimestamp);
   }

   public boolean putFromLoad(SessionImplementor session, Object key, Object value, long txTimestamp, Object version) throws CacheException {
      return delegate.putFromLoad(session, key, value, txTimestamp, version);
   }

   public boolean putFromLoad(SessionImplementor session, Object key, Object value, long txTimestamp, Object version, boolean minimalPutOverride) throws CacheException {
      return delegate.putFromLoad(session, key, value, txTimestamp, version, minimalPutOverride);
   }

   public SoftLock lockItem(SessionImplementor session, Object key, Object version) throws CacheException {
      return null;
   }

   public void unlockItem(SessionImplementor session, Object key, SoftLock lock) throws CacheException {
      delegate.unlockItem(session, key);
   }

   public void remove(SessionImplementor session, Object key) throws CacheException {
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

   public CollectionRegion getRegion() {
      return region;
   }

}
