package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.impl.BaseQueryFactory;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class EmbeddedQueryFactory extends BaseQueryFactory {

   private final QueryEngine queryEngine;

   public EmbeddedQueryFactory(QueryEngine queryEngine) {
      this.queryEngine = queryEngine;
   }

   @Override
   public Query create(String queryString) {
      //todo [anistor] some params come from a builder, some from parsing the query string
      return new DelegatingQuery(queryEngine, this, queryString, null, null, -1, -1);
   }

   @Override
   public QueryBuilder from(Class type) {
      return new EmbeddedQueryBuilder(this, queryEngine, type.getName());
   }

   @Override
   public QueryBuilder from(String type) {
      return new EmbeddedQueryBuilder(this, queryEngine, type);
   }
}
