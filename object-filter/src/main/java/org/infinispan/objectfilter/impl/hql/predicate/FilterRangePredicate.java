package org.infinispan.objectfilter.impl.hql.predicate;

import org.hibernate.hql.ast.spi.predicate.ComparisonPredicate;
import org.hibernate.hql.ast.spi.predicate.RangePredicate;
import org.infinispan.objectfilter.impl.syntax.AndExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.ComparisonExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantValueExpr;
import org.infinispan.objectfilter.impl.syntax.PropertyValueExpr;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class FilterRangePredicate extends RangePredicate<BooleanExpr> {

   public FilterRangePredicate(String propertyName, Object lower, Object upper) {
      super(propertyName, lower, upper);
   }

   @Override
   public BooleanExpr getQuery() {
      return new AndExpr(
            new ComparisonExpr(new PropertyValueExpr(propertyName), new ConstantValueExpr(lower), ComparisonPredicate.Type.GREATER_OR_EQUAL),
            new ComparisonExpr(new PropertyValueExpr(propertyName), new ConstantValueExpr(upper), ComparisonPredicate.Type.LESS_OR_EQUAL)
      );
   }
}
