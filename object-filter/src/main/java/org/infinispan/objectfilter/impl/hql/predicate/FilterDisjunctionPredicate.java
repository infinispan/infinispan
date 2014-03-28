package org.infinispan.objectfilter.impl.hql.predicate;

import org.hibernate.hql.ast.spi.predicate.DisjunctionPredicate;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.OrExpr;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class FilterDisjunctionPredicate extends DisjunctionPredicate<BooleanExpr> {

   public FilterDisjunctionPredicate() {
   }

   @Override
   public BooleanExpr getQuery() {
      // we always expect children.size() >= 1
      BooleanExpr firstChild = children.get(0).getQuery();
      if (children.size() == 1) {
         return firstChild;
      }

      OrExpr orExpr = new OrExpr(firstChild);

      for (int i = 1; i < children.size(); i++) {
         BooleanExpr child = children.get(i).getQuery();
         orExpr.getChildren().add(child);
      }

      return orExpr;
   }
}
