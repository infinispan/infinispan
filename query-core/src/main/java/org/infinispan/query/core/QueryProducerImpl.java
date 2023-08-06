package org.infinispan.query.core;

import org.infinispan.cache.impl.QueryProducer;
import org.infinispan.commons.api.query.Query;
import org.infinispan.query.core.impl.EmbeddedQueryFactory;
import org.infinispan.query.core.impl.QueryEngine;

public final class QueryProducerImpl implements QueryProducer {

   private EmbeddedQueryFactory queryFactory;

   public QueryProducerImpl(QueryEngine<?> queryEngine) {
      queryFactory = new EmbeddedQueryFactory(queryEngine);
   }

   public void upgrade(QueryEngine<?> queryEngine) {
      this.queryFactory = new EmbeddedQueryFactory(queryEngine);
   }

   @Override
   public <T> Query<T> query(String query) {
      return queryFactory.create(query);
   }
}
