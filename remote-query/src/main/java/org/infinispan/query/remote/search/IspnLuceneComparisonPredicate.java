package org.infinispan.query.remote.search;

import org.apache.lucene.search.Query;
import org.hibernate.hql.ast.spi.predicate.ComparisonPredicate;
import org.hibernate.search.bridge.util.impl.NumericFieldUtils;
import org.hibernate.search.query.dsl.QueryBuilder;

/**
 * Lucene-based comparison predicate.
 *
 * @author Gunnar Morling
 */
class IspnLuceneComparisonPredicate extends ComparisonPredicate<Query> {

   private final QueryBuilder builder;

   public IspnLuceneComparisonPredicate(QueryBuilder builder, String propertyName, Type comparisonType, Object value) {
      super(propertyName, comparisonType, value);
      this.builder = builder;
   }

   @Override
   protected Query getStrictlyLessQuery() {
      if (IspnLucenePredicateFactory.isNumericValue(value)) {
         return NumericFieldUtils.createNumericRangeQuery(
               propertyName,
               null,
               value,
               false,
               false
         );
      }
      return builder.range().onField(propertyName).ignoreFieldBridge().below(value).excludeLimit().createQuery();
   }

   @Override
   protected Query getLessOrEqualsQuery() {
      if (IspnLucenePredicateFactory.isNumericValue(value)) {
         return NumericFieldUtils.createNumericRangeQuery(
               propertyName,
               null,
               value,
               false,
               true
         );
      }
      return builder.range().onField(propertyName).ignoreFieldBridge().below(value).createQuery();
   }

   @Override
   protected Query getEqualsQuery() {
      if (IspnLucenePredicateFactory.isNumericValue(value)) {
         return NumericFieldUtils.createExactMatchQuery(propertyName, value);
      }
      return builder.keyword().onField(propertyName).ignoreFieldBridge().matching(value).createQuery();
   }

   @Override
   protected Query getGreaterOrEqualsQuery() {
      if (IspnLucenePredicateFactory.isNumericValue(value)) {
         return NumericFieldUtils.createNumericRangeQuery(
               propertyName,
               value,
               null,
               true,
               false
         );
      }
      return builder.range().onField(propertyName).ignoreFieldBridge().above(value).createQuery();
   }

   @Override
   protected Query getStrictlyGreaterQuery() {
      if (IspnLucenePredicateFactory.isNumericValue(value)) {
         return NumericFieldUtils.createNumericRangeQuery(
               propertyName,
               value,
               null,
               false,
               false
         );
      }
      return builder.range().onField(propertyName).ignoreFieldBridge().above(value).excludeLimit().createQuery();
   }
}
