package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.dsl.impl.BaseQueryFactory;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class EmbeddedQueryFactory extends BaseQueryFactory {

   private final QueryEngine<?> queryEngine;

   public EmbeddedQueryFactory(QueryEngine queryEngine) {
      if (queryEngine == null) {
         throw new IllegalArgumentException("queryEngine cannot be null");
      }
      this.queryEngine = queryEngine;
   }

   @Override
   public BaseQuery create(String queryString) {
      return new DelegatingQuery<>(queryEngine, this, queryString, IndexedQueryMode.FETCH);
   }

   @Override
   public Query create(String queryString, IndexedQueryMode queryMode) {
      return new DelegatingQuery<>(queryEngine, this, queryString, queryMode);
   }

   @Override
   public QueryBuilder from(Class<?> type) {
      return new EmbeddedQueryBuilder(this, queryEngine, type.getName());
   }

   @Override
   public QueryBuilder from(String type) {
      return new EmbeddedQueryBuilder(this, queryEngine, type);
   }
}
