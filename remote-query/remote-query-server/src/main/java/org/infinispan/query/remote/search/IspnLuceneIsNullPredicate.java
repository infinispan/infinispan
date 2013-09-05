package org.infinispan.query.remote.search;

import org.apache.lucene.search.Query;
import org.hibernate.hql.ast.spi.predicate.IsNullPredicate;
import org.hibernate.search.query.dsl.QueryBuilder;

/**
 * Lucene-based {@code IS NULL} predicate.
 *
 * @author Gunnar Morling
 */
class IspnLuceneIsNullPredicate extends IsNullPredicate<Query> {

   private final QueryBuilder builder;
   private final String nullToken;

   public IspnLuceneIsNullPredicate(QueryBuilder builder, String propertyName, String nullToken) {
      super(propertyName);
      this.builder = builder;
      this.nullToken = nullToken;
   }

   @Override
   public Query getQuery() {
      return builder.keyword().onField(propertyName).ignoreFieldBridge().matching(nullToken).createQuery();
   }
}