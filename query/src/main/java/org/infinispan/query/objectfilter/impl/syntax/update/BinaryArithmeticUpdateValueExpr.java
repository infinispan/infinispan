package org.infinispan.query.objectfilter.impl.syntax.update;

import org.infinispan.query.functions.QueryFunctionContext;
import org.infinispan.query.objectfilter.impl.syntax.ConstantValueExpr;

/**
 * A binary arithmetic operation ({@code + - * / %}) in an UPDATE statement
 * value expression. The operands may be {@link UpdateValueExpr} trees or
 * {@link ConstantValueExpr.ParamPlaceholder} instances.
 *
 * @since 16.4
 */
public final class BinaryArithmeticUpdateValueExpr implements UpdateValueExpr {

   private final ArithmeticOperator operator;
   private final Object left;
   private final Object right;

   public BinaryArithmeticUpdateValueExpr(ArithmeticOperator operator, Object left, Object right) {
      this.operator = operator;
      this.left = left;
      this.right = right;
   }

   public ArithmeticOperator getOperator() {
      return operator;
   }

   public Object getLeft() {
      return left;
   }

   public Object getRight() {
      return right;
   }

   @Override
   public Object evaluate(QueryFunctionContext ctx) {
      Object l = UpdateValueEvaluator.evaluateOperand(left, ctx);
      Object r = UpdateValueEvaluator.evaluateOperand(right, ctx);
      return UpdateValueEvaluator.applyArithmetic(operator, l, r);
   }

   @Override
   public void appendQueryString(StringBuilder sb) {
      sb.append('(');
      appendOperand(sb, left);
      sb.append(operator.getSymbol());
      appendOperand(sb, right);
      sb.append(')');
   }

   private static void appendOperand(StringBuilder sb, Object operand) {
      if (operand instanceof UpdateValueExpr expr) {
         expr.appendQueryString(sb);
      } else {
         sb.append(operand);
      }
   }
}
