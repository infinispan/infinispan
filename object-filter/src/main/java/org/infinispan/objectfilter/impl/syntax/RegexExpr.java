package org.infinispan.objectfilter.impl.syntax;

import java.util.regex.Pattern;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class RegexExpr implements PrimaryPredicateExpr {

   private final ValueExpr child;
   private final Pattern pattern;

   public RegexExpr(ValueExpr child, String pattern) {
      this.child = child;
      this.pattern = Pattern.compile(pattern);
   }

   @Override
   public ValueExpr getChild() {
      return child;
   }

   public Pattern getPattern() {
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
