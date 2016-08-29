package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class FullTextBoostExpr implements BooleanExpr {

   private final BooleanExpr child;

   private final float boost;

   public FullTextBoostExpr(BooleanExpr child, float boost) {
      this.child = child;
      this.boost = boost;
   }

   public float getBoost() {
      return boost;
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
      return "(" + child + ")^" + boost;
   }

   @Override
   public String toQueryString() {
      return "(" + child.toQueryString() + ")^" + boost;
   }
}
