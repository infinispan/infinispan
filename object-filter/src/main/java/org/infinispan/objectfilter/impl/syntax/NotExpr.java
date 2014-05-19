package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class NotExpr implements BooleanExpr {

   private BooleanExpr child;

   public NotExpr(BooleanExpr child) {
      this.child = child;
   }


   public BooleanExpr getChild() {
      return child;
   }

   @Override
   public BooleanExpr acceptVisitor(Visitor visitor) {
      return visitor.visit(this);
   }

   @Override
   public String toString() {
      return "NotExpr(" + child + ')';
   }
}
