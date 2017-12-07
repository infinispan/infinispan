package org.infinispan.hibernate.cache.v51.query;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.engine.spi.SessionImplementor;
import org.infinispan.AdvancedCache;
import org.infinispan.hibernate.cache.commons.InfinispanRegionFactory;

import javax.transaction.TransactionManager;

public final class QueryResultsRegionImpl
   extends org.infinispan.hibernate.cache.commons.query.QueryResultsRegionImpl
   implements QueryResultsRegion {

   public QueryResultsRegionImpl(AdvancedCache cache, String name, TransactionManager transactionManager, InfinispanRegionFactory factory) {
      super(cache, name, transactionManager, factory);
   }

   @Override
   public Object get(SessionImplementor session, Object key) throws CacheException {
      return getItem(session, key);
   }

   @Override
   public void put(SessionImplementor session, Object key, Object value) throws CacheException {
      putItem(session, key, value);
   }

}
