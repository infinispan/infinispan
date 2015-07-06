package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class LikeExpr implements PrimaryPredicateExpr {

   private final ValueExpr child;
   private final String pattern;

   public LikeExpr(ValueExpr child, String pattern) {
      this.child = child;
      this.pattern = pattern;
   }

   @Override
   public ValueExpr getChild() {
      return child;
   }

   public String getPattern() {
      return pattern;
   }

   @Override
   public BooleanExpr acceptVisitor(Visitor visitor) {
      return visitor.visit(this);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LikeExpr likeExpr = (LikeExpr) o;
      return pattern.equals(likeExpr.pattern) && child.equals(likeExpr.child);
   }

   @Override
   public int hashCode() {
      return 31 * child.hashCode() + pattern.hashCode();
   }

   @Override
   public String toString() {
      return "LIKE(" + child + ", " + pattern + ')';
   }

   @Override
   public String toJpaString() {
      return child.toJpaString() + " LIKE " + pattern;
   }
}
