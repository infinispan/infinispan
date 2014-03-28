package org.infinispan.objectfilter.impl.hql.predicate;

import org.hibernate.hql.ast.spi.predicate.NegationPredicate;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.NotExpr;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class FilterNegationPredicate extends NegationPredicate<BooleanExpr> {

   public FilterNegationPredicate() {
   }

   @Override
   public BooleanExpr getQuery() {
      return new NotExpr(getChild().getQuery());
   }
}
