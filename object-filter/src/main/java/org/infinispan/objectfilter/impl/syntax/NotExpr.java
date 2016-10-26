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
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
   }

   @Override
   public String toString() {
      return "NOT(" + child + ')';
   }

   @Override
   public String toQueryString() {
      return "NOT(" + child.toQueryString() + ")";
   }
}
