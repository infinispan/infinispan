package org.infinispan.objectfilter.impl.hql.predicate;

import org.hibernate.hql.ast.spi.predicate.RootPredicate;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class FilterRootPredicate extends RootPredicate<BooleanExpr> {

   public FilterRootPredicate() {
   }

   @Override
   public BooleanExpr getQuery() {
      return child == null ? null : child.getQuery();
   }
}
