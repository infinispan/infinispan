package org.infinispan.hibernate.cache.main.naturalid;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;

public class ReadWriteAccess extends ReadOnlyAccess {

   public ReadWriteAccess(NaturalIdRegionImpl region, AccessDelegate delegate) {
      super(region, delegate);
   }

   @Override
   public boolean update(SharedSessionContractImplementor session, Object key, Object value) throws CacheException {
      return delegate.update( session, key, value, null, null );
   }

   @Override
   public boolean afterUpdate(SharedSessionContractImplementor session, Object key, Object value, SoftLock lock) throws CacheException {
      return delegate.afterUpdate( session, key, value, null, null, lock );
   }

}
