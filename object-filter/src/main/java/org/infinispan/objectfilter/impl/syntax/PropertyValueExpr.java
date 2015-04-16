package org.infinispan.objectfilter.impl.syntax;

import org.infinispan.objectfilter.impl.util.StringHelper;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class PropertyValueExpr implements ValueExpr {

   private final List<String> propertyPath;

   public PropertyValueExpr(List<String> propertyPath) {
      this.propertyPath = propertyPath;
   }

   public PropertyValueExpr(String propertyPath) {
      this(StringHelper.splitPropertyPath(propertyPath));
   }

   public List<String> getPropertyPath() {
      return propertyPath;
   }

   @Override
   public ValueExpr acceptVisitor(Visitor visitor) {
      return visitor.visit(this);
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
      return "PropertyValueExpr(" + propertyPath + ')';
   }
}
