package org.infinispan.objectfilter.impl.syntax;

import org.infinispan.objectfilter.PropertyPath;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class AggregationExpr extends PropertyValueExpr {

   private final PropertyPath propertyPath;

   public AggregationExpr(PropertyPath.AggregationType aggregationType, String[] propertyPath, boolean isRepeated, Class<?> primitiveType) {
      super(propertyPath, isRepeated, primitiveType);
      if (aggregationType == null) {
         throw new IllegalArgumentException("aggregationType cannot be null");
      }
      this.propertyPath = new PropertyPath(aggregationType, propertyPath);
   }

   public PropertyPath getAggregationPath() {
      return propertyPath;
   }

   public PropertyPath.AggregationType getAggregationType() {
      return propertyPath.getAggregationType();
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
      return propertyPath.getAggregationType().name() + "(" + super.toString() + ")";
   }

   @Override
   public String toQueryString() {
      return propertyPath.getAggregationType().name() + "(" + super.toQueryString() + ")";
   }

   @Override
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
   }
}
