package org.infinispan.query.remote.search;

import org.apache.lucene.search.Query;
import org.hibernate.hql.ast.spi.predicate.InPredicate;
import org.hibernate.hql.lucene.internal.builder.predicate.LuceneDisjunctionPredicate;
import org.hibernate.search.query.dsl.QueryBuilder;

import java.util.List;

/**
 * Lucene-based {@code IN} predicate in the form of disjoint {@code EQUALS} predicates for the given values.
 *
 * @author Gunnar Morling
 */
class IspnLuceneInPredicate extends InPredicate<Query> {

   private final QueryBuilder builder;

   public IspnLuceneInPredicate(QueryBuilder builder, String propertyName, List<Object> values) {
      super(propertyName, values);
      this.builder = builder;
   }

   @Override
   public Query getQuery() {
      LuceneDisjunctionPredicate predicate = new LuceneDisjunctionPredicate(builder);

      for (Object element : values) {
         IspnLuceneComparisonPredicate equals = new IspnLuceneComparisonPredicate(
               builder,
               propertyName,
               org.hibernate.hql.ast.spi.predicate.ComparisonPredicate.Type.EQUALS, element);

         predicate.add(equals);
      }

      return predicate.getQuery();
   }
}
