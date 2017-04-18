package org.infinispan.objectfilter.impl.syntax;

import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class LikeExpr implements PrimaryPredicateExpr {

   public static final char SINGLE_CHARACTER_WILDCARD = '_';

   public static final char MULTIPLE_CHARACTERS_WILDCARD = '%';

   public static final char DEFAULT_ESCAPE_CHARACTER = '\\';

   private final ValueExpr child;
   private final Object pattern;
   private final char escapeChar = DEFAULT_ESCAPE_CHARACTER;

   public LikeExpr(ValueExpr child, Object pattern) {
      this.child = child;
      this.pattern = pattern;
   }

   @Override
   public ValueExpr getChild() {
      return child;
   }

   public String getPattern(Map<String, Object> namedParameters) {
      if (pattern instanceof ConstantValueExpr.ParamPlaceholder) {
         String paramName = ((ConstantValueExpr.ParamPlaceholder) pattern).getName();
         if (namedParameters == null) {
            throw new IllegalStateException("Missing value for parameter " + paramName);
         }
         String p = (String) namedParameters.get(paramName);
         if (p == null) {
            throw new IllegalStateException("Missing value for parameter " + paramName);
         }
         return p;
      } else {
         return (String) pattern;
      }
   }

   public char getEscapeChar() {
      return escapeChar;
   }

   @Override
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
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
   public String toQueryString() {
      return child.toQueryString() + " LIKE '" + pattern + "'";
   }
}
