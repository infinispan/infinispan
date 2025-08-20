package org.infinispan.query.objectfilter.impl.syntax;

/**
 * An expression that represents a comparison of Comparable values.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ComparisonExpr implements PrimaryPredicateExpr {

   private final ValueExpr leftChild;
   private final ValueExpr rightChild;
   private final Type type;

   public enum Type {
      LESS,
      LESS_OR_EQUAL,
      EQUAL,
      NOT_EQUAL,
      GREATER_OR_EQUAL,
      GREATER;

      public Type negate() {
         return switch (this) {
            case LESS -> GREATER_OR_EQUAL;
            case LESS_OR_EQUAL -> GREATER;
            case EQUAL -> NOT_EQUAL;
            case NOT_EQUAL -> EQUAL;
            case GREATER_OR_EQUAL -> LESS;
            case GREATER -> LESS_OR_EQUAL;
            default -> this;
         };
      }

      public Type reverse() {
         return switch (this) {
            case LESS -> GREATER;
            case GREATER -> LESS;
            case LESS_OR_EQUAL -> GREATER_OR_EQUAL;
            case GREATER_OR_EQUAL -> LESS_OR_EQUAL;
            default -> this;
         };
      }
   }

   public ComparisonExpr(ValueExpr leftChild, ValueExpr rightChild, Type type) {
      this.leftChild = leftChild;
      this.rightChild = rightChild;
      this.type = type;
   }

   public ValueExpr getLeftChild() {
      return leftChild;
   }

   public ValueExpr getRightChild() {
      return rightChild;
   }

   public Type getComparisonType() {
      return type;
   }

   @Override
   public ValueExpr getChild() {
      return leftChild;
   }

   @Override
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ComparisonExpr other = (ComparisonExpr) o;
      return type == other.type && leftChild.equals(other.leftChild) && rightChild.equals(other.rightChild);
   }

   @Override
   public int hashCode() {
      int result = 31 * leftChild.hashCode() + rightChild.hashCode();
      return 31 * result + type.hashCode();
   }

   @Override
   public String toString() {
      return type + "(" + leftChild + ", " + rightChild + ')';
   }

   @Override
   public void appendQueryString(StringBuilder sb) {
      leftChild.appendQueryString(sb);
      sb.append(' ');
      switch (type) {
         case LESS:
            sb.append('<');
            break;
         case LESS_OR_EQUAL:
            sb.append("<=");
            break;
         case EQUAL:
            sb.append('=');
            break;
         case NOT_EQUAL:
            sb.append("!=");
            break;
         case GREATER_OR_EQUAL:
            sb.append(">=");
            break;
         case GREATER:
            sb.append('>');
            break;
      }
      sb.append(' ');
      rightChild.appendQueryString(sb);
   }
}
