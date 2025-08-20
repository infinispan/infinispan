package org.infinispan.query.objectfilter.impl.syntax;

import java.util.List;
import java.util.Objects;

public final class SpatialWithinPolygonExpr implements PrimaryPredicateExpr {

   private final ValueExpr leftChild;
   private final List<ConstantValueExpr> vector;

   public SpatialWithinPolygonExpr(ValueExpr leftChild, List<ConstantValueExpr> vector) {
      this.leftChild = leftChild;
      this.vector = vector;
   }

   @Override
   public ValueExpr getChild() {
      return leftChild;
   }

   public ValueExpr getLeftChild() {
      return leftChild;
   }

   public List<ConstantValueExpr> getVector() {
      return vector;
   }

   @Override
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SpatialWithinPolygonExpr that = (SpatialWithinPolygonExpr) o;
      return Objects.equals(leftChild, that.leftChild) && Objects.equals(vector, that.vector);
   }

   @Override
   public int hashCode() {
      return Objects.hash(leftChild, vector);
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      appendQueryString(sb);
      return sb.toString();
   }

   @Override
   public void appendQueryString(StringBuilder sb) {
      leftChild.appendQueryString(sb);
      sb.append(" WITHIN POLYGON ").append(vector);
   }
}
