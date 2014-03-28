package org.infinispan.objectfilter.impl.hql.predicate;

import org.hibernate.hql.ast.spi.predicate.ConjunctionPredicate;
import org.infinispan.objectfilter.impl.syntax.AndExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class FilterConjunctionPredicate extends ConjunctionPredicate<BooleanExpr> {

   public FilterConjunctionPredicate() {
   }

   @Override
   public BooleanExpr getQuery() {
      // we always expect children.size() >= 1
      BooleanExpr firstChild = children.get(0).getQuery();
      if (children.size() == 1) {
         return firstChild;
      }

      AndExpr andExpr = new AndExpr(firstChild);

      for (int i = 1; i < children.size(); i++) {
         BooleanExpr child = children.get(i).getQuery();
         andExpr.getChildren().add(child);
      }

      return andExpr;
   }
}
