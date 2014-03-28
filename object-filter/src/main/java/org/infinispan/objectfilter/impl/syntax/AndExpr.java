package org.infinispan.objectfilter.impl.syntax;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class AndExpr extends BooleanOperatorExpr {

   public AndExpr(BooleanExpr... children) {
      super(children);
   }

   public AndExpr(List<BooleanExpr> children) {
      super(children);
   }

   @Override
   public BooleanExpr acceptVisitor(Visitor visitor) {
      return visitor.visit(this);
   }

   @Override
   public String toString() {
      return "AndExpr{children=" + children + '}';
   }
}
