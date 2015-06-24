package org.infinispan.objectfilter.impl.hql.predicate;

import org.hibernate.hql.ast.spi.predicate.RangePredicate;
import org.infinispan.objectfilter.impl.syntax.AndExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.ComparisonExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantValueExpr;
import org.infinispan.objectfilter.impl.syntax.ValueExpr;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class FilterRangePredicate extends RangePredicate<BooleanExpr> {

   private final ValueExpr valueExpr;

   public FilterRangePredicate(ValueExpr valueExpr, Object lower, Object upper) {
      super(valueExpr.toJpaString(), lower, upper);
      this.valueExpr = valueExpr;
   }

   @Override
   public BooleanExpr getQuery() {
      return new AndExpr(
            new ComparisonExpr(valueExpr, new ConstantValueExpr((Comparable) lower), ComparisonExpr.Type.GREATER_OR_EQUAL),
            new ComparisonExpr(valueExpr, new ConstantValueExpr((Comparable) upper), ComparisonExpr.Type.LESS_OR_EQUAL)
      );
   }
}
