package org.infinispan.objectfilter.impl.syntax;

import org.infinispan.objectfilter.impl.util.StringHelper;

import java.util.Arrays;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class PropertyValueExpr implements ValueExpr {

   protected final String[] propertyPath;

   protected final boolean isRepeated;

   protected final Class<?> primitiveType;

   public PropertyValueExpr(String[] propertyPath, boolean isRepeated, Class<?> primitiveType) {
      this.propertyPath = propertyPath;
      this.isRepeated = isRepeated;
      this.primitiveType = primitiveType;
   }

   public PropertyValueExpr(String propertyPath, boolean isRepeated, Class<?> primitiveType) {
      this(StringHelper.split(propertyPath), isRepeated, primitiveType);
   }

   public String[] getPropertyPath() {
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
      return Arrays.equals(propertyPath, other.propertyPath);
   }

   @Override
   public int hashCode() {
      return Arrays.hashCode(propertyPath);
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
