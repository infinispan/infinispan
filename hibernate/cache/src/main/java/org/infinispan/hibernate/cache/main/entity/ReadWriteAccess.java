package org.infinispan.hibernate.cache.main.entity;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;

class ReadWriteAccess extends ReadOnlyAccess {

   ReadWriteAccess(EntityRegionImpl region, AccessDelegate delegate) {
      super(region, delegate);
   }

   public boolean update(SharedSessionContractImplementor session, Object key, Object value, Object currentVersion, Object previousVersion) throws CacheException {
      return delegate.update(session, key, value, currentVersion, previousVersion);
   }

   public boolean afterUpdate(SharedSessionContractImplementor session, Object key, Object value, Object currentVersion, Object previousVersion, SoftLock lock) throws CacheException {
      return delegate.afterUpdate(session, key, value, currentVersion, previousVersion, lock);
   }

}
