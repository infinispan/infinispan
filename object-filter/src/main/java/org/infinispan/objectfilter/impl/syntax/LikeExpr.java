package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class LikeExpr implements PrimaryPredicateExpr {

   public static final char SINGLE_CHARACTER_WILDCARD = '_';

   public static final char MULTIPLE_CHARACTERS_WILDCARD = '%';

   public static final char DEFAULT_ESCAPE_CHARACTER = '\\';

   private final ValueExpr child;
   private final String pattern;
   private final char escapeChar = DEFAULT_ESCAPE_CHARACTER;

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
   public String toJpaString() {
      return child.toJpaString() + " LIKE '" + pattern + "'";
   }
}
