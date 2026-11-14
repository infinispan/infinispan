package org.infinispan.query.objectfilter.impl.syntax.update;

import org.infinispan.query.functions.QueryFunctionContext;
import org.infinispan.query.objectfilter.impl.syntax.ConstantValueExpr;

/**
 * A unary minus in an UPDATE statement value expression. The operand may be
 * an {@link UpdateValueExpr} tree or a {@link ConstantValueExpr.ParamPlaceholder}.
 *
 * @since 16.4
 */
public final class NegateUpdateValueExpr implements UpdateValueExpr {

   private final Object operand;

   public NegateUpdateValueExpr(Object operand) {
      this.operand = operand;
   }

   public Object getOperand() {
      return operand;
   }

   @Override
   public Object evaluate(QueryFunctionContext ctx) {
      Object value = UpdateValueEvaluator.evaluateOperand(operand, ctx);
      return UpdateValueEvaluator.applyNegate(value);
   }

   @Override
   public void appendQueryString(StringBuilder sb) {
      sb.append('(').append('-');
      if (operand instanceof UpdateValueExpr expr) {
         expr.appendQueryString(sb);
      } else {
         sb.append(operand);
      }
      sb.append(')');
   }
}
