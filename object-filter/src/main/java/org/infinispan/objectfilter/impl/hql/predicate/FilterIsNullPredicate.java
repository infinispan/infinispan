package org.infinispan.objectfilter.impl.hql.predicate;

import org.hibernate.hql.ast.spi.predicate.IsNullPredicate;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.IsNullExpr;
import org.infinispan.objectfilter.impl.syntax.ValueExpr;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class FilterIsNullPredicate extends IsNullPredicate<BooleanExpr> {

   private final ValueExpr valueExpr;

   public FilterIsNullPredicate(ValueExpr valueExpr) {
      super(valueExpr.toJpaString());
      this.valueExpr = valueExpr;
   }

   @Override
   public BooleanExpr getQuery() {
      return new IsNullExpr(valueExpr);
   }
}
