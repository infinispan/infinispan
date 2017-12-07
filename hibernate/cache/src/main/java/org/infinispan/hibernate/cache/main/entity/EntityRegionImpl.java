package org.infinispan.hibernate.cache.main.entity;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.EntityRegionAccessStrategy;
import org.infinispan.AdvancedCache;
import org.infinispan.hibernate.cache.commons.InfinispanRegionFactory;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;
import org.infinispan.hibernate.cache.commons.impl.BaseTransactionalDataRegion;
import org.infinispan.hibernate.cache.main.access.SessionAccessImpl;

import javax.transaction.TransactionManager;

public class EntityRegionImpl extends BaseTransactionalDataRegion implements EntityRegion {

   public EntityRegionImpl(
      AdvancedCache cache, String name, TransactionManager transactionManager,
      CacheDataDescription metadata, InfinispanRegionFactory factory, CacheKeysFactory cacheKeysFactory) {
      super(cache, name, transactionManager, metadata, factory, cacheKeysFactory);
   }

   @Override
   public EntityRegionAccessStrategy buildAccessStrategy(AccessType accessType) throws CacheException {
      checkAccessType(accessType);
      AccessDelegate accessDelegate = createAccessDelegate(accessType);
      if ( accessType == AccessType.READ_ONLY || !getCacheDataDescription().isMutable() ) {
         return new ReadOnlyAccess( this, accessDelegate );
      }
      else {
         return new ReadWriteAccess( this, accessDelegate );
      }
   }

}
