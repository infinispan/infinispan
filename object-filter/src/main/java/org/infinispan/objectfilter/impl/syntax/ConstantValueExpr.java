package org.infinispan.objectfilter.impl.syntax;

/**
 * A constant comparable value, to be used as right or left side in a comparison expression.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ConstantValueExpr implements ValueExpr {

   private final Comparable constantValue;

   public ConstantValueExpr(Comparable constantValue) {
      if (constantValue == null) {
         throw new IllegalArgumentException("constantValue cannot be null");
      }
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
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ConstantValueExpr other = (ConstantValueExpr) o;
      return constantValue.equals(other.constantValue);
   }

   @Override
   public int hashCode() {
      return constantValue.hashCode();
   }

   @Override
   public String toString() {
      return "CONST(" + constantValue + ')';
   }

   @Override
   public String toJpaString() {
      if (constantValue instanceof String) {
         return "\"" + constantValue + "\"";
      }
      if (constantValue instanceof Character) {
         return "'" + constantValue + "'";
      }
      return "" + constantValue;
   }
}
