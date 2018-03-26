package org.infinispan.hibernate.cache.main.naturalid;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.NaturalIdRegion;
import org.hibernate.cache.spi.access.NaturalIdRegionAccessStrategy;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;
import org.infinispan.hibernate.cache.main.access.AbstractAccess;

public class ReadOnlyAccess extends AbstractAccess implements NaturalIdRegionAccessStrategy {

   private final NaturalIdRegionImpl region;

   public ReadOnlyAccess(NaturalIdRegionImpl region, AccessDelegate delegate) {
      super(delegate);
      this.region = region;
   }

   @Override
   public boolean insert(SharedSessionContractImplementor session, Object key, Object value) throws CacheException {
      return delegate.insert(session, key, value, null);
   }

   @Override
   public boolean update(SharedSessionContractImplementor session, Object key, Object value) throws CacheException {
      throw new UnsupportedOperationException("Illegal attempt to edit read only item");
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
   public boolean afterInsert(SharedSessionContractImplementor session, Object key, Object value) throws CacheException {
      return delegate.afterInsert(session, key, value, null);
   }

   @Override
   public boolean afterUpdate(SharedSessionContractImplementor session, Object key, Object value, SoftLock lock) throws CacheException {
      throw new UnsupportedOperationException("Illegal attempt to edit read only item");
   }

   @Override
   public Object generateCacheKey(Object[] naturalIdValues, EntityPersister persister, SharedSessionContractImplementor session) {
      return region.getCacheKeysFactory().createNaturalIdKey(naturalIdValues, persister, session);
   }

   @Override
   public Object[] getNaturalIdValues(Object cacheKey) {
      return region.getCacheKeysFactory().getNaturalIdValues(cacheKey);
   }

   @Override
   public NaturalIdRegion getRegion() {
      return region;
   }
}
