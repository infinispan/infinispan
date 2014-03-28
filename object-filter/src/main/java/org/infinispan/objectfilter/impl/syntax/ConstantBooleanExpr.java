package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ConstantBooleanExpr implements PrimaryPredicateExpr {

   public static final ConstantBooleanExpr TRUE = new ConstantBooleanExpr(true);

   public static final ConstantBooleanExpr FALSE = new ConstantBooleanExpr(false);

   public static ConstantBooleanExpr forBoolean(boolean value) {
      return value ? TRUE : FALSE;
   }

   private final boolean constantValue;

   private ConstantBooleanExpr(boolean constantValue) {
      this.constantValue = constantValue;
   }

   @Override
   public ValueExpr getChild() {
      return null;
   }

   public boolean getValue() {
      return constantValue;
   }

   public ConstantBooleanExpr negate() {
      return constantValue ? FALSE : TRUE;
   }

   @Override
   public BooleanExpr acceptVisitor(Visitor visitor) {
      return visitor.visit(this);
   }

   @Override
   public String toString() {
      return "ConstantBooleanExpr(" + constantValue + ')';
   }
}
