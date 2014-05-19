package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class IsNullExpr implements PrimaryPredicateExpr {

   private final ValueExpr child;

   public IsNullExpr(ValueExpr child) {
      this.child = child;
   }

   @Override
   public ValueExpr getChild() {
      return child;
   }

   @Override
   public BooleanExpr acceptVisitor(Visitor visitor) {
      return visitor.visit(this);
   }

   @Override
   public String toString() {
      return "IsNullExpr(" + child + ')';
   }
}
