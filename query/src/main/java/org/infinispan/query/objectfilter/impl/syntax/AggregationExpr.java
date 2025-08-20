package org.infinispan.query.objectfilter.impl.syntax;

import org.infinispan.query.objectfilter.impl.ql.AggregationFunction;
import org.infinispan.query.objectfilter.impl.syntax.parser.AggregationPropertyPath;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class AggregationExpr extends PropertyValueExpr {

   private final AggregationPropertyPath<?> propertyPath;

   public AggregationExpr(AggregationPropertyPath<?> propertyPath, boolean isRepeated, Class<?> primitiveType) {
      super(propertyPath, isRepeated, primitiveType);
      this.propertyPath = propertyPath;
   }

   public AggregationFunction getAggregationType() {
      return propertyPath.getAggregationFunction();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AggregationExpr other = (AggregationExpr) o;
      return propertyPath.equals(other.propertyPath);
   }

   @Override
   public int hashCode() {
      return propertyPath.hashCode();
   }

   @Override
   public String toString() {
      return propertyPath.getAggregationFunction().name() + "(" + super.toString() + ")";
   }

   @Override
   public void appendQueryString(StringBuilder sb) {
      sb.append(propertyPath.getAggregationFunction().name());
      sb.append("(");
      super.appendQueryString(sb);
      sb.append(")");
   }

   @Override
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
   }
}
