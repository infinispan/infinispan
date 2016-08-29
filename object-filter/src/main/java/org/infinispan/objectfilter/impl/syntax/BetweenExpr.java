package org.infinispan.objectfilter.impl.syntax;

/**
 * An expression that represents a range of Comparable values corresponding to the BETWEEN predicate. The lower and
 * upper bound are included.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class BetweenExpr implements PrimaryPredicateExpr {

   private final ValueExpr leftChild;

   private final ValueExpr fromChild;

   private final ValueExpr toChild;

   public BetweenExpr(ValueExpr leftChild, ValueExpr fromChild, ValueExpr toChild) {
      this.leftChild = leftChild;
      this.fromChild = fromChild;
      this.toChild = toChild;
   }

   @Override
   public ValueExpr getChild() {
      return leftChild;
   }

   public ValueExpr getLeftChild() {
      return leftChild;
   }

   public ValueExpr getFromChild() {
      return fromChild;
   }

   public ValueExpr getToChild() {
      return toChild;
   }

   @Override
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      BetweenExpr other = (BetweenExpr) o;
      return leftChild.equals(other.leftChild)
            && fromChild.equals(other.fromChild)
            && toChild.equals(other.toChild);
   }

   @Override
   public int hashCode() {
      return 31 * (31 * leftChild.hashCode() + fromChild.hashCode()) + toChild.hashCode();
   }

   @Override
   public String toString() {
      return "BETWEEN(" + leftChild + ", " + fromChild + ", " + toChild + ")";
   }

   @Override
   public String toQueryString() {
      return leftChild.toQueryString() + " BETWEEN " + fromChild.toQueryString() + " AND " + toChild.toQueryString();
   }
}
