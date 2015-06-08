package org.infinispan.objectfilter.impl.syntax;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class OrExpr extends BooleanOperatorExpr {

   public OrExpr(BooleanExpr... children) {
      super(children);
   }

   public OrExpr(List<BooleanExpr> children) {
      super(children);
   }

   @Override
   public BooleanExpr acceptVisitor(Visitor visitor) {
      return visitor.visit(this);
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("OR(");
      boolean isFirst = true;
      for (BooleanExpr c : children) {
         if (isFirst) {
            isFirst = false;
         } else {
            sb.append(", ");
         }
         sb.append(c);
      }
      sb.append(")");
      return sb.toString();
   }

   @Override
   public String toJpaString() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < children.size(); i++) {
         if (i != 0) {
            sb.append(" OR ");
         }
         sb.append("(").append(children.get(i).toJpaString()).append(")");
      }
      return sb.toString();
   }
}
