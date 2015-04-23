package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.embedded.LuceneQuery;
import org.infinispan.query.dsl.impl.BaseQueryFactory;

//TODO remove this class until 8.0.Final

/**
 * @author anistor@redhat.com
 * @since 6.0
 * @deprecated replaced by {@link EmbeddedQueryFactory}
 */
public final class EmbeddedLuceneQueryFactory extends BaseQueryFactory<LuceneQuery> {

   private final QueryEngine queryEngine;

   public EmbeddedLuceneQueryFactory(QueryEngine queryEngine) {
      this.queryEngine = queryEngine;
   }

   @Override
   public QueryBuilder<LuceneQuery> from(Class type) {
      return new EmbeddedLuceneQueryBuilder(this, queryEngine, type.getCanonicalName());
   }

   @Override
   public QueryBuilder<LuceneQuery> from(String type) {
      return new EmbeddedLuceneQueryBuilder(this, queryEngine, type);
   }
}
