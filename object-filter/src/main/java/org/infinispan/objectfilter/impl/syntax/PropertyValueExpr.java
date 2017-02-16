package org.infinispan.objectfilter.impl.syntax;

import org.infinispan.objectfilter.impl.ql.PropertyPath;

/**
 * A property reference expression.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public class PropertyValueExpr implements ValueExpr {

   protected final PropertyPath<?> propertyPath;

   protected final boolean isRepeated;

   protected final Class<?> primitiveType;

   public PropertyValueExpr(PropertyPath<?> propertyPath, boolean isRepeated, Class<?> primitiveType) {
      this.propertyPath = propertyPath;
      this.isRepeated = isRepeated;
      this.primitiveType = primitiveType;
   }

   public PropertyValueExpr(String propertyPath, boolean isRepeated, Class<?> primitiveType) {
      this(PropertyPath.make(propertyPath), isRepeated, primitiveType);
   }

   public PropertyPath<?> getPropertyPath() {
      return propertyPath;
   }

   public boolean isRepeated() {
      return isRepeated;
   }

   public Class<?> getPrimitiveType() {
      return primitiveType;
   }

   @Override
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      PropertyValueExpr other = (PropertyValueExpr) o;
      return propertyPath.equals(other.propertyPath);
   }

   @Override
   public int hashCode() {
      return propertyPath.hashCode();
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("PROP(").append(propertyPath);
      if (isRepeated) {
         sb.append('*');
      }
      sb.append(')');
      return sb.toString();
   }

   @Override
   public String toQueryString() {
      return propertyPath.toString();
   }
}
