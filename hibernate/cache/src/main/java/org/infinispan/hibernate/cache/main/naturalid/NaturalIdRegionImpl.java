package org.infinispan.hibernate.cache.main.naturalid;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.cache.spi.NaturalIdRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.NaturalIdRegionAccessStrategy;
import org.infinispan.AdvancedCache;
import org.infinispan.hibernate.cache.commons.InfinispanRegionFactory;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;
import org.infinispan.hibernate.cache.commons.impl.BaseTransactionalDataRegion;

import javax.transaction.TransactionManager;

public class NaturalIdRegionImpl extends BaseTransactionalDataRegion implements NaturalIdRegion {

   public NaturalIdRegionImpl(AdvancedCache cache, String name, TransactionManager transactionManager, CacheDataDescription metadata, InfinispanRegionFactory factory, CacheKeysFactory cacheKeysFactory) {
      super(cache, name, transactionManager, metadata, factory, cacheKeysFactory);
   }

   @Override
   public NaturalIdRegionAccessStrategy buildAccessStrategy(AccessType accessType) throws CacheException {
      checkAccessType( accessType );
      AccessDelegate accessDelegate = createAccessDelegate(accessType);
      if ( accessType == AccessType.READ_ONLY || !getCacheDataDescription().isMutable() ) {
         return new ReadOnlyAccess( this, accessDelegate );
      }
      else {
         return new ReadWriteAccess( this, accessDelegate );
      }
   }

}
