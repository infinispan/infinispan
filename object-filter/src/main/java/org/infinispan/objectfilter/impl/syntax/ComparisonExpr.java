package org.infinispan.objectfilter.impl.syntax;

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
         switch (this) {
            case LESS:
               return GREATER_OR_EQUAL;
            case LESS_OR_EQUAL:
               return GREATER;
            case EQUAL:
               return NOT_EQUAL;
            case NOT_EQUAL:
               return EQUAL;
            case GREATER_OR_EQUAL:
               return LESS;
            case GREATER:
               return LESS_OR_EQUAL;
            default:
               return this;
         }
      }

      public Type reverse() {
         switch (this) {
            case LESS:
               return GREATER;
            case GREATER:
               return LESS;
            case LESS_OR_EQUAL:
               return GREATER_OR_EQUAL;
            case GREATER_OR_EQUAL:
               return LESS_OR_EQUAL;
            default:
               return this;
         }
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
   public BooleanExpr acceptVisitor(Visitor visitor) {
      return visitor.visit(this);
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
   public String toJpaString() {
      StringBuilder sb = new StringBuilder();
      sb.append(leftChild.toJpaString()).append(' ');
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
      sb.append(' ').append(rightChild.toJpaString());
      return sb.toString();
   }
}
