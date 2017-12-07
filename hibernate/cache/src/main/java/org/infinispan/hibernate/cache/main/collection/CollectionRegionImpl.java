package org.infinispan.hibernate.cache.main.collection;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.cache.spi.CollectionRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.CollectionRegionAccessStrategy;
import org.infinispan.AdvancedCache;
import org.infinispan.hibernate.cache.commons.InfinispanRegionFactory;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;
import org.infinispan.hibernate.cache.commons.impl.BaseTransactionalDataRegion;

import javax.transaction.TransactionManager;

public class CollectionRegionImpl extends BaseTransactionalDataRegion implements CollectionRegion {

   public CollectionRegionImpl(
      AdvancedCache cache, String name, TransactionManager transactionManager,
      CacheDataDescription metadata, InfinispanRegionFactory factory, CacheKeysFactory cacheKeysFactory) {
      super(cache, name, transactionManager, metadata, factory, cacheKeysFactory);
   }

   @Override
   public CollectionRegionAccessStrategy buildAccessStrategy(AccessType accessType) throws CacheException {
      checkAccessType(accessType);
      AccessDelegate accessDelegate = createAccessDelegate(accessType);
      return new CollectionAccess(this, accessDelegate);
   }

}
