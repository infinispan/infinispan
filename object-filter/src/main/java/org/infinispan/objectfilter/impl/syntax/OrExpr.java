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
}
