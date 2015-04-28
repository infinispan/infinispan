package org.infinispan.objectfilter.impl.hql.predicate;

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

   private final boolean isRepeatedProperty;

   public FilterRangePredicate(String propertyName, boolean isRepeatedProperty, Object lower, Object upper) {
      super(propertyName, lower, upper);
      this.isRepeatedProperty = isRepeatedProperty;
   }

   @Override
   public BooleanExpr getQuery() {
      return new AndExpr(
            new ComparisonExpr(new PropertyValueExpr(propertyName, isRepeatedProperty), new ConstantValueExpr((Comparable) lower), ComparisonExpr.Type.GREATER_OR_EQUAL),
            new ComparisonExpr(new PropertyValueExpr(propertyName, isRepeatedProperty), new ConstantValueExpr((Comparable) upper), ComparisonExpr.Type.LESS_OR_EQUAL)
      );
   }
}
