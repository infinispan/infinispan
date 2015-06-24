package org.infinispan.objectfilter.impl.hql.predicate;

import org.hibernate.hql.ast.spi.predicate.LikePredicate;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.LikeExpr;
import org.infinispan.objectfilter.impl.syntax.ValueExpr;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class FilterLikePredicate extends LikePredicate<BooleanExpr> {

   private final ValueExpr valueExpr;

   public FilterLikePredicate(ValueExpr valueExpr, String patternValue, Character escapeCharacter) {
      super(valueExpr.toJpaString(), patternValue, escapeCharacter);
      this.valueExpr = valueExpr;
   }

   @Override
   public BooleanExpr getQuery() {
      return new LikeExpr(valueExpr, patternValue);
   }
}
