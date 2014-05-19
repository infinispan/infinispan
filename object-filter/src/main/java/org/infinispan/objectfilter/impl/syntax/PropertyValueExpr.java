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
   public String toString() {
      return "PropertyValueExpr(" + propertyPath + ')';
   }
}
