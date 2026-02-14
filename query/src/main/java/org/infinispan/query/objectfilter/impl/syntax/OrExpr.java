package org.infinispan.query.objectfilter.impl.syntax;

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
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
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
      sb.append(')');
      return sb.toString();
   }

   @Override
   public void appendQueryString(StringBuilder sb) {
      String commonField = getCommonFullTextField();
      if (commonField != null) {
         sb.append(commonField).append(" : (");
         for (int i = 0; i < children.size(); i++) {
            if (i != 0) {
               sb.append(" || ");
            }
            appendFullTextInner(sb, children.get(i));
         }
         sb.append(')');
      } else {
         for (int i = 0; i < children.size(); i++) {
            if (i != 0) {
               sb.append(" OR ");
            }
            sb.append('(');
            children.get(i).appendQueryString(sb);
            sb.append(')');
         }
      }
   }
}
