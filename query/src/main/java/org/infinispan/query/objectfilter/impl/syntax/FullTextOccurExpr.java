package org.infinispan.query.objectfilter.impl.syntax;

import org.infinispan.query.objectfilter.impl.syntax.parser.VirtualExpressionBuilder;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class FullTextOccurExpr implements BooleanExpr {

   private final BooleanExpr child;

   private final VirtualExpressionBuilder.Occur occur;

   public FullTextOccurExpr(BooleanExpr child, VirtualExpressionBuilder.Occur occur) {
      this.child = child;
      this.occur = occur;
   }

   public VirtualExpressionBuilder.Occur getOccur() {
      return occur;
   }

   public BooleanExpr getChild() {
      return child;
   }

   @Override
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
   }

   @Override
   public String toString() {
      return occur + "(" + child + ")";
   }

   @Override
   public void appendQueryString(StringBuilder sb) {
      sb.append(((VirtualExpressionBuilder.Occur) occur).getOperator());
      child.appendQueryString(sb);
   }
}
