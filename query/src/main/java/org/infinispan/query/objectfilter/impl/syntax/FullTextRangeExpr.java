package org.infinispan.query.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class FullTextRangeExpr implements PrimaryPredicateExpr {

   private final ValueExpr leftChild;

   private final boolean includeLower;

   private final Object lower;

   private final boolean includeUpper;

   private final Object upper;

   public FullTextRangeExpr(ValueExpr leftChild, boolean includeLower, Object lower, Object upper, boolean includeUpper) {
      this.leftChild = leftChild;
      this.includeLower = includeLower;
      this.lower = lower;
      this.upper = upper;
      this.includeUpper = includeUpper;
   }

   public boolean isIncludeLower() {
      return includeLower;
   }

   public boolean isIncludeUpper() {
      return includeUpper;
   }

   public Object getLower() {
      return lower;
   }

   public Object getUpper() {
      return upper;
   }

   @Override
   public String toString() {
      return leftChild.toString() + ":" +
            (includeLower ? '[' : '{') + (lower == null ? "*" : lower) +
            " TO " +
            (upper == null ? "*" : upper) + (includeUpper ? ']' : '}');
   }

   @Override
   public void appendQueryString(StringBuilder sb) {
      leftChild.appendQueryString(sb);
      sb.append(":");
      if (includeLower) {
         sb.append('[');
      } else {
         sb.append('{');
      }
      sb.append(" TO ");
      if (upper == null) {
         sb.append("*");
      }
      if (includeUpper) {
         sb.append(']');
      } else {
         sb.append('}');
      }
   }

   @Override
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
   }

   @Override
   public ValueExpr getChild() {
      return leftChild;
   }
}
