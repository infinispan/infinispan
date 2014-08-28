package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ConstantValueExpr implements ValueExpr {

   private final Comparable constantValue;

   public ConstantValueExpr(Comparable constantValue) {
      this.constantValue = constantValue;
   }

   public Comparable getConstantValue() {
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
