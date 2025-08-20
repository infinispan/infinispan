package org.infinispan.query.objectfilter.impl.syntax;

import java.util.Collections;
import java.util.List;

public class NestedExpr extends BooleanOperatorExpr {

   private final String nestedPath;

   @Override
   public void appendQueryString(StringBuilder sb) {
      sb.append("NESTED ( ");
      for (int i = 0; i < children.size(); i++) {
         if (i != 0) {
            sb.append(" AND ");
         }
         sb.append('(');
         children.get(i).appendQueryString(sb);
         sb.append(')');
      }
      sb.append(" ) ");
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("NESTED(");
      boolean isFirst = true;
      for (BooleanExpr c : children) {
         if (isFirst) {
            isFirst = false;
         } else {
            sb.append(", ");
         }
         sb.append(c);
      }
      sb.append(')');
      return sb.toString();
   }

   @Override
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
   }

   public NestedExpr(String nestedPath, BooleanExpr... nestedChildExpressions) {
      this.nestedPath= nestedPath;
      Collections.addAll(this.children, nestedChildExpressions);
   }

   public List<BooleanExpr> getNestedChildren() {
      return children;
   }

   public String getNestedPath() {
      return nestedPath;
   }

   public void add(BooleanExpr expr) {
      children.add(expr);

   }
}
