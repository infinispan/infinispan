package org.infinispan.objectfilter.impl.hql.predicate;

import org.hibernate.hql.ast.spi.predicate.LikePredicate;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.LikeExpr;
import org.infinispan.objectfilter.impl.syntax.PropertyValueExpr;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class FilterLikePredicate extends LikePredicate<BooleanExpr> {

   public FilterLikePredicate(String propertyName, String patternValue) {
      super(propertyName, patternValue, null);
   }

   @Override
   public BooleanExpr getQuery() {
      return new LikeExpr(new PropertyValueExpr(propertyName), patternValue);
   }
}
