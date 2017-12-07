package org.infinispan.hibernate.cache.commons;

import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.cache.spi.CollectionRegion;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.NaturalIdRegion;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.infinispan.AdvancedCache;
import org.infinispan.hibernate.cache.commons.impl.BaseRegion;

import javax.transaction.TransactionManager;

public interface InternalRegionFactory {

   <T extends BaseRegion & CollectionRegion> T createCollectionRegion(
      AdvancedCache cache, String regionName, TransactionManager transactionManager,
      CacheDataDescription metadata, InfinispanRegionFactory regionFactory,
      CacheKeysFactory cacheKeysFactory);

   <T extends BaseRegion & EntityRegion> T createEntityRegion(
      AdvancedCache cache, String regionName, TransactionManager transactionManager,
      CacheDataDescription metadata, InfinispanRegionFactory regionFactory,
      CacheKeysFactory cacheKeysFactory);

   <T extends BaseRegion & NaturalIdRegion> T createNaturalIdRegion(
      AdvancedCache cache, String regionName, TransactionManager transactionManager,
      CacheDataDescription metadata, InfinispanRegionFactory regionFactory,
      CacheKeysFactory cacheKeysFactory);

   <T extends BaseRegion & QueryResultsRegion> T createQueryResultsRegion(
      AdvancedCache cache, String regionName, TransactionManager transactionManager,
      InfinispanRegionFactory regionFactory);

}
