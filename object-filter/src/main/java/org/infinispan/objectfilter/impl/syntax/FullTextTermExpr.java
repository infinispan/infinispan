package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class FullTextTermExpr implements PrimaryPredicateExpr {

   private final ValueExpr leftChild;

   private final String term;

   private final Integer fuzzySlop;

   public FullTextTermExpr(ValueExpr leftChild, String term, Integer fuzzySlop) {
      this.leftChild = leftChild;
      this.term = term;
      this.fuzzySlop = fuzzySlop;
   }

   public String getTerm() {
      return term;
   }

   public Integer getFuzzySlop() {
      return fuzzySlop;
   }

   @Override
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
   }

   @Override
   public ValueExpr getChild() {
      return leftChild;
   }

   @Override
   public String toString() {
      return leftChild.toString() + ":'" + term + "'" + (fuzzySlop != null ? "~" + fuzzySlop : "");
   }

   @Override
   public String toQueryString() {
      return leftChild.toQueryString() + ":'" + term + "'" + (fuzzySlop != null ? "~" + fuzzySlop : "");
   }
}
