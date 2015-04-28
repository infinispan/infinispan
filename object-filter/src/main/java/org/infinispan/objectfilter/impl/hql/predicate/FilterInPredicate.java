package org.infinispan.objectfilter.impl.hql.predicate;

import org.hibernate.hql.ast.spi.predicate.ComparisonPredicate;
import org.hibernate.hql.ast.spi.predicate.InPredicate;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class FilterInPredicate extends InPredicate<BooleanExpr> {

   private final boolean isRepeatedProperty;

   public FilterInPredicate(String propertyName, boolean isRepeatedProperty, List<Object> values) {
      super(propertyName, values);
      this.isRepeatedProperty = isRepeatedProperty;
   }

   @Override
   public BooleanExpr getQuery() {
      FilterDisjunctionPredicate predicate = new FilterDisjunctionPredicate();

      for (Object element : values) {
         //todo need efficient implementation
         FilterComparisonPredicate eq = new FilterComparisonPredicate(propertyName, isRepeatedProperty, ComparisonPredicate.Type.EQUALS, element);
         predicate.add(eq);
      }

      return predicate.getQuery();
   }
}
