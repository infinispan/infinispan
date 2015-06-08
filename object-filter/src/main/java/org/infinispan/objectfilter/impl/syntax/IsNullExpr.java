package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class IsNullExpr implements PrimaryPredicateExpr {

   private final ValueExpr child;

   public IsNullExpr(ValueExpr child) {
      this.child = child;
   }

   @Override
   public ValueExpr getChild() {
      return child;
   }

   @Override
   public BooleanExpr acceptVisitor(Visitor visitor) {
      return visitor.visit(this);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      IsNullExpr other = (IsNullExpr) o;
      return child.equals(other.child);
   }

   @Override
   public int hashCode() {
      return child.hashCode();
   }

   @Override
   public String toString() {
      return "IS_NULL(" + child + ')';
   }

   @Override
   public String toJpaString() {
      return child.toString() + " IS NULL";
   }
}
