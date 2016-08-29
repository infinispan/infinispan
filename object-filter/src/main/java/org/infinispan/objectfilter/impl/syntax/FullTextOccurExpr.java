package org.infinispan.objectfilter.impl.syntax;

import org.infinispan.objectfilter.impl.ql.QueryRendererDelegate;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class FullTextOccurExpr implements BooleanExpr {

   private final BooleanExpr child;

   private final QueryRendererDelegate.Occur occur;

   public FullTextOccurExpr(BooleanExpr child, QueryRendererDelegate.Occur occur) {
      this.child = child;
      this.occur = occur;
   }

   public QueryRendererDelegate.Occur getOccur() {
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
   public String toQueryString() {
      return occur.getOperator() + child.toQueryString();
   }
}
