package org.infinispan.hibernate.cache.v51;

import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.infinispan.AdvancedCache;
import org.infinispan.hibernate.cache.commons.InfinispanRegionFactory;
import org.infinispan.hibernate.cache.commons.InternalRegionFactory;
import org.infinispan.hibernate.cache.v51.collection.CollectionRegionImpl;
import org.infinispan.hibernate.cache.v51.entity.EntityRegionImpl;
import org.infinispan.hibernate.cache.v51.naturalid.NaturalIdRegionImpl;
import org.infinispan.hibernate.cache.v51.query.QueryResultsRegionImpl;

import javax.transaction.TransactionManager;

public class InternalRegionFactoryImpl implements InternalRegionFactory {

   @Override
   @SuppressWarnings("unchecked")
   public CollectionRegionImpl createCollectionRegion(AdvancedCache cache, String regionName, TransactionManager transactionManager, CacheDataDescription metadata, InfinispanRegionFactory regionFactory, CacheKeysFactory cacheKeysFactory) {
      return new CollectionRegionImpl(cache, regionName, transactionManager, metadata, regionFactory, cacheKeysFactory);
   }

   @Override
   @SuppressWarnings("unchecked")
   public EntityRegionImpl createEntityRegion(AdvancedCache cache, String regionName, TransactionManager transactionManager, CacheDataDescription metadata, InfinispanRegionFactory regionFactory, CacheKeysFactory cacheKeysFactory) {
      return new EntityRegionImpl(cache, regionName, transactionManager, metadata, regionFactory, cacheKeysFactory);
   }

   @Override
   @SuppressWarnings("unchecked")
   public NaturalIdRegionImpl createNaturalIdRegion(AdvancedCache cache, String regionName, TransactionManager transactionManager, CacheDataDescription metadata, InfinispanRegionFactory regionFactory, CacheKeysFactory cacheKeysFactory) {
      return new NaturalIdRegionImpl(cache, regionName, transactionManager, metadata, regionFactory, cacheKeysFactory);
   }

   @Override
   @SuppressWarnings("unchecked")
   public QueryResultsRegionImpl createQueryResultsRegion(AdvancedCache cache, String regionName, TransactionManager transactionManager, InfinispanRegionFactory regionFactory) {
      return new QueryResultsRegionImpl(cache, regionName, transactionManager, regionFactory);
   }

}
