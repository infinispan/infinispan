package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.impl.BaseQueryFactory;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class EmbeddedQueryFactory extends BaseQueryFactory<Query> {

   private final QueryEngine queryEngine;

   public EmbeddedQueryFactory(QueryEngine queryEngine) {
      this.queryEngine = queryEngine;
   }

   @Override
   public QueryBuilder<Query> from(Class type) {
      return new EmbeddedQueryBuilder(this, queryEngine, type.getCanonicalName());
   }

   @Override
   public QueryBuilder<Query> from(String type) {
      return new EmbeddedQueryBuilder(this, queryEngine, type);
   }
}
