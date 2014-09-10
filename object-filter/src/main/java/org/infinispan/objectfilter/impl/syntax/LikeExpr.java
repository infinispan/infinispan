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
   public String toString() {
      return "RegexExpr{" +
            "child=" + child +
            ", pattern=" + pattern +
            '}';
   }
}
