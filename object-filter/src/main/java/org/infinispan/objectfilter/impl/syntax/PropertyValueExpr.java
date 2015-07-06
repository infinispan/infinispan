package org.infinispan.objectfilter.impl.syntax;

import org.infinispan.objectfilter.impl.util.StringHelper;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class PropertyValueExpr implements ValueExpr {

   private final List<String> propertyPath;

   private final boolean isRepeated;

   public PropertyValueExpr(List<String> propertyPath, boolean isRepeated) {
      this.propertyPath = propertyPath;
      this.isRepeated = isRepeated;
   }

   public PropertyValueExpr(String propertyPath, boolean isRepeated) {
      this(StringHelper.splitPropertyPath(propertyPath), isRepeated);
   }

   public List<String> getPropertyPath() {
      return propertyPath;
   }

   public boolean isRepeated() {
      return isRepeated;
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
      StringBuilder sb = new StringBuilder();
      sb.append("PROP(");
      boolean isFirst = true;
      for (String p : propertyPath) {
         if (isFirst) {
            isFirst = false;
         } else {
            sb.append(',');
         }
         sb.append(p);
      }
      if (isRepeated) {
         sb.append('*');
      }
      sb.append(')');
      return sb.toString();
   }

   @Override
   public String toJpaString() {
      StringBuilder sb = new StringBuilder();
      boolean isFirst = true;
      for (String p : propertyPath) {
         if (isFirst) {
            isFirst = false;
         } else {
            sb.append('.');
         }
         sb.append(p);
      }
      return sb.toString();
   }
}
