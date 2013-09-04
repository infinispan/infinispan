package org.infinispan.query.remote.search;

import org.apache.lucene.search.Query;
import org.hibernate.hql.ast.spi.predicate.RangePredicate;
import org.hibernate.search.bridge.util.impl.NumericFieldUtils;
import org.hibernate.search.query.dsl.QueryBuilder;

/**
 * Lucene-based {@code BETWEEN} predicate.
 *
 * @author Gunnar Morling
 */
public class IspnLuceneRangePredicate extends RangePredicate<Query> {

   private final QueryBuilder builder;

   public IspnLuceneRangePredicate(QueryBuilder builder, String propertyName, Object lower, Object upper) {
      super(propertyName, lower, upper);
      this.builder = builder;
   }

   @Override
   public Query getQuery() {
      if (IspnLucenePredicateFactory.isNumericValue(lower) || IspnLucenePredicateFactory.isNumericValue(upper)) {
         return NumericFieldUtils.createNumericRangeQuery(
               propertyName,
               lower,
               upper,
               true,
               true
         );
      }
      return builder.range().onField(propertyName).ignoreFieldBridge().from(lower).to(upper).createQuery();
   }
}
