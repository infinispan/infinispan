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

   private final boolean isRepeatedProperty;

   public FilterComparisonPredicate(String propertyName, boolean isRepeatedProperty, Type comparisonType, Object comparisonValue) {
      super(propertyName, comparisonType, comparisonValue);
      this.isRepeatedProperty = isRepeatedProperty;
   }

   @Override
   protected BooleanExpr getStrictlyLessQuery() {
      return new ComparisonExpr(new PropertyValueExpr(propertyName, isRepeatedProperty), new ConstantValueExpr((Comparable) value), ComparisonExpr.Type.LESS);
   }

   @Override
   protected BooleanExpr getLessOrEqualsQuery() {
      return new ComparisonExpr(new PropertyValueExpr(propertyName, isRepeatedProperty), new ConstantValueExpr((Comparable) value), ComparisonExpr.Type.LESS_OR_EQUAL);
   }

   @Override
   protected BooleanExpr getEqualsQuery() {
      return new ComparisonExpr(new PropertyValueExpr(propertyName, isRepeatedProperty), new ConstantValueExpr((Comparable) value), ComparisonExpr.Type.EQUAL);
   }

   @Override
   protected BooleanExpr getGreaterOrEqualsQuery() {
      return new ComparisonExpr(new PropertyValueExpr(propertyName, isRepeatedProperty), new ConstantValueExpr((Comparable) value), ComparisonExpr.Type.GREATER_OR_EQUAL);
   }

   @Override
   protected BooleanExpr getStrictlyGreaterQuery() {
      return new ComparisonExpr(new PropertyValueExpr(propertyName, isRepeatedProperty), new ConstantValueExpr((Comparable) value), ComparisonExpr.Type.GREATER);
   }
}
