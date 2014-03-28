package org.infinispan.objectfilter.impl.hql.predicate;

import org.hibernate.hql.ast.spi.predicate.ComparisonPredicate;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.ComparisonExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantValueExpr;
import org.infinispan.objectfilter.impl.syntax.PropertyValueExpr;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class FilterComparisonPredicate extends ComparisonPredicate<BooleanExpr> {

   public FilterComparisonPredicate(String propertyName, Type comparisonType, Object comparisonValue) {
      super(propertyName, comparisonType, comparisonValue);
   }

   @Override
   protected BooleanExpr getStrictlyLessQuery() {
      return new ComparisonExpr(new PropertyValueExpr(propertyName), new ConstantValueExpr(value), type);
   }

   @Override
   protected BooleanExpr getLessOrEqualsQuery() {
      return new ComparisonExpr(new PropertyValueExpr(propertyName), new ConstantValueExpr(value), type);
   }

   @Override
   protected BooleanExpr getEqualsQuery() {
      return new ComparisonExpr(new PropertyValueExpr(propertyName), new ConstantValueExpr(value), type);
   }

   @Override
   protected BooleanExpr getGreaterOrEqualsQuery() {
      return new ComparisonExpr(new PropertyValueExpr(propertyName), new ConstantValueExpr(value), type);
   }

   @Override
   protected BooleanExpr getStrictlyGreaterQuery() {
      return new ComparisonExpr(new PropertyValueExpr(propertyName), new ConstantValueExpr(value), type);
   }
}
