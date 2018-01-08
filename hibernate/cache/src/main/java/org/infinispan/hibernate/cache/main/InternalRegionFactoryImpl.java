package org.infinispan.hibernate.cache.main;

import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.infinispan.AdvancedCache;
import org.infinispan.hibernate.cache.commons.InfinispanRegionFactory;
import org.infinispan.hibernate.cache.commons.InternalRegionFactory;
import org.infinispan.hibernate.cache.main.collection.CollectionRegionImpl;
import org.infinispan.hibernate.cache.main.entity.EntityRegionImpl;
import org.infinispan.hibernate.cache.main.naturalid.NaturalIdRegionImpl;
import org.infinispan.hibernate.cache.main.query.QueryResultsRegionImpl;
import org.infinispan.hibernate.cache.main.timestamp.ClusteredTimestampsRegionImpl;
import org.infinispan.hibernate.cache.main.timestamp.TimestampsRegionImpl;
import org.kohsuke.MetaInfServices;

import javax.transaction.TransactionManager;

@MetaInfServices(InternalRegionFactory.class)
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

   @Override
   @SuppressWarnings("unchecked")
   public TimestampsRegionImpl createTimestampsRegion(AdvancedCache cache, String regionName, InfinispanRegionFactory regionFactory) {
      return new TimestampsRegionImpl(cache, regionName, regionFactory);
   }

   @Override
   @SuppressWarnings("unchecked")
   public ClusteredTimestampsRegionImpl createClusteredTimestampsRegion(AdvancedCache cache, String regionName, InfinispanRegionFactory regionFactory) {
      return new ClusteredTimestampsRegionImpl(cache, regionName, regionFactory);
   }

}
