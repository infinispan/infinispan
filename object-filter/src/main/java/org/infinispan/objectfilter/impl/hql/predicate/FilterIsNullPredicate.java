package org.infinispan.objectfilter.impl.hql.predicate;

import org.hibernate.hql.ast.spi.predicate.IsNullPredicate;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.IsNullExpr;
import org.infinispan.objectfilter.impl.syntax.PropertyValueExpr;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class FilterIsNullPredicate extends IsNullPredicate<BooleanExpr> {

   public FilterIsNullPredicate(String propertyName) {
      super(propertyName);
   }

   @Override
   public BooleanExpr getQuery() {
      return new IsNullExpr(new PropertyValueExpr(propertyName));
   }
}
