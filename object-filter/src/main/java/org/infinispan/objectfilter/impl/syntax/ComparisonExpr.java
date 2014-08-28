package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ComparisonExpr implements PrimaryPredicateExpr {

   private final ValueExpr leftChild;
   private final ValueExpr rightChild;
   private final Type comparisonType;

   public enum Type {
      LESS,
      LESS_OR_EQUAL,
      EQUAL,
      NOT_EQUAL,
      GREATER_OR_EQUAL,
      GREATER
   }

   public ComparisonExpr(ValueExpr leftChild, ValueExpr rightChild, Type comparisonType) {
      this.leftChild = leftChild;
      this.rightChild = rightChild;
      this.comparisonType = comparisonType;
   }

   public ValueExpr getLeftChild() {
      return leftChild;
   }

   public ValueExpr getRightChild() {
      return rightChild;
   }

   public Type getComparisonType() {
      return comparisonType;
   }

   @Override
   public ValueExpr getChild() {
      return leftChild;
   }

   @Override
   public BooleanExpr acceptVisitor(Visitor visitor) {
      return visitor.visit(this);
   }

   @Override
   public String toString() {
      return "ComparisonExpr{" +
            " comparisonType=" + comparisonType +
            ", leftChild=" + leftChild +
            ", rightChild=" + rightChild +
            '}';
   }
}
