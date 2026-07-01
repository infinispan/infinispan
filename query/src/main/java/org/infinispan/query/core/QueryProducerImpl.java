package org.infinispan.query.core;

import org.infinispan.Cache;
import org.infinispan.cache.impl.QueryProducer;
import org.infinispan.commons.api.query.ContinuousQuery;
import org.infinispan.commons.api.query.Query;
import org.infinispan.query.core.impl.EmbeddedQueryFactory;
import org.infinispan.query.core.impl.continuous.ContinuousQueryImpl;
import org.infinispan.query.impl.QueryEngine;

public final class QueryProducerImpl implements QueryProducer {

   private final EmbeddedQueryFactory queryFactory;

   public QueryProducerImpl(QueryEngine<?> queryEngine) {
      queryFactory = new EmbeddedQueryFactory(queryEngine);
   }

   @Override
   public <T> Query<T> query(String query) {
      return queryFactory.create(query);
   }

   @Override
   public <K, V> ContinuousQuery<K, V> continuousQuery(Cache<K, V> cache) {
      return new ContinuousQueryImpl<>(cache);
   }
}
