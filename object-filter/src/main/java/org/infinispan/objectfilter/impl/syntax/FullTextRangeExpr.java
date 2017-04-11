package org.infinispan.objectfilter.impl.syntax;

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
   public String toQueryString() {
      return leftChild.toQueryString() + ":" +
            (includeLower ? '[' : '{') + (lower == null ? "*" : lower) +
            " TO " +
            (upper == null ? "*" : upper) + (includeUpper ? ']' : '}');
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
