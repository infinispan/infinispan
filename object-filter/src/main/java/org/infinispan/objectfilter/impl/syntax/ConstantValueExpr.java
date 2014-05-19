package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ConstantValueExpr implements ValueExpr {

   private final Object constantValue;

   public ConstantValueExpr(Object constantValue) {
      this.constantValue = constantValue;
   }

   public Object getConstantValue() {
      return constantValue;
   }

   @Override
   public ValueExpr acceptVisitor(Visitor visitor) {
      return visitor.visit(this);
   }

   @Override
   public String toString() {
      return "ConstantValueExpr(" + constantValue + ')';
   }
}
