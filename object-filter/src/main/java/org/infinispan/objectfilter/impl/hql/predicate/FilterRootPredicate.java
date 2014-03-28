package org.infinispan.objectfilter.impl.hql.predicate;

import org.hibernate.hql.ast.spi.predicate.RootPredicate;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantBooleanExpr;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class FilterRootPredicate extends RootPredicate<BooleanExpr> {

   public FilterRootPredicate() {
   }

   @Override
   public BooleanExpr getQuery() {
      return child == null ? ConstantBooleanExpr.TRUE : child.getQuery();
   }
}
