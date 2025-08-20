package org.infinispan.query.objectfilter.impl.syntax;

import java.util.Objects;

public final class SpatialWithinBoxExpr implements PrimaryPredicateExpr {

   private final ValueExpr leftChild;
   private final ValueExpr tlLatChild;
   private final ValueExpr tlLonChild;
   private final ValueExpr brLatChild;
   private final ValueExpr brLonChild;

   public SpatialWithinBoxExpr(ValueExpr leftChild, ValueExpr tlLatChild, ValueExpr tlLonChild,
                               ValueExpr brLatChild, ValueExpr brLonChild) {
      this.leftChild = leftChild;
      this.tlLatChild = tlLatChild;
      this.tlLonChild = tlLonChild;
      this.brLatChild = brLatChild;
      this.brLonChild = brLonChild;
   }

   @Override
   public ValueExpr getChild() {
      return leftChild;
   }

   public ValueExpr getLeftChild() {
      return leftChild;
   }

   public ValueExpr getTlLatChild() {
      return tlLatChild;
   }

   public ValueExpr getTlLonChild() {
      return tlLonChild;
   }

   public ValueExpr getBrLatChild() {
      return brLatChild;
   }

   public ValueExpr getBrLonChild() {
      return brLonChild;
   }

   @Override
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SpatialWithinBoxExpr that = (SpatialWithinBoxExpr) o;
      return Objects.equals(leftChild, that.leftChild) && Objects.equals(tlLatChild, that.tlLatChild) && Objects.equals(tlLonChild, that.tlLonChild) && Objects.equals(brLatChild, that.brLatChild) && Objects.equals(brLonChild, that.brLonChild);
   }

   @Override
   public int hashCode() {
      return Objects.hash(leftChild, tlLatChild, tlLonChild, brLatChild, brLonChild);
   }

   @Override
   public String toString() {
      return "WITHIN_BOX(" + leftChild + ", " + tlLatChild + ", " + tlLonChild + ", " + brLatChild + ", " + brLonChild + ")";
   }

   @Override
   public String toQueryString() {
      return leftChild.toQueryString() + " WITHIN BOX( "
            + tlLatChild.toQueryString() + ", " + tlLonChild.toQueryString()
            + ", " + brLatChild.toQueryString() + ", " + brLonChild.toQueryString() + ")";
   }

   @Override
   public void appendQueryString(StringBuilder sb) {
      leftChild.appendQueryString(sb);
      sb.append(" WITHIN BOX( ");
      tlLatChild.appendQueryString(sb);
      sb.append(", ");
      tlLonChild.appendQueryString(sb);
      sb.append(", ");
      brLatChild.appendQueryString(sb);
      sb.append(", ");
      brLonChild.appendQueryString(sb);
      sb.append(")");
   }
}
