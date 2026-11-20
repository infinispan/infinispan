package org.infinispan.query.objectfilter.impl.syntax.update;

import java.util.Map;

import org.infinispan.query.functions.QueryFunctionContext;
import org.infinispan.query.objectfilter.impl.syntax.ConstantValueExpr;

/**
 * Tree-walking interpreter for UPDATE statement value expressions.
 * <p>
 * Semantics:
 * <ul>
 * <li>numeric arithmetic: both operands integral (byte/short/int/long) uses
 * {@code long} math with Java overflow-wrap semantics; if either operand is
 * floating point the result is a {@code double}.</li>
 * <li>{@code +} with any {@code String} operand performs string concatenation.</li>
 * <li>null propagation: any {@code null} operand yields a {@code null} result.</li>
 * <li>division or modulo by zero is an error.</li>
 * </ul>
 *
 * @since 16.4
 */
public final class UpdateValueEvaluator {

   private UpdateValueEvaluator() {
   }

   /**
    * Evaluates an expression tree.
    */
   public static Object evaluate(UpdateValueExpr expr, QueryFunctionContext ctx) {
      return expr.evaluate(ctx);
   }

   /**
    * Resolves a single operand of an expression: an expression tree, a named
    * parameter placeholder or a raw constant.
    */
   public static Object evaluateOperand(Object operand, QueryFunctionContext ctx) {
      if (operand instanceof UpdateValueExpr expr) {
         return expr.evaluate(ctx);
      }
      if (operand instanceof ConstantValueExpr.ParamPlaceholder placeholder) {
         return resolveParameter(placeholder, ctx);
      }
      return operand;
   }

   static Object resolveParameter(ConstantValueExpr.ParamPlaceholder placeholder, QueryFunctionContext ctx) {
      Map<String, Object> namedParameters = ctx.getNamedParameters();
      if (namedParameters == null || !namedParameters.containsKey(placeholder.getName())) {
         throw new IllegalArgumentException("Missing value for parameter: " + placeholder.getName());
      }
      return namedParameters.get(placeholder.getName());
   }

   /**
    * Applies a binary arithmetic operation on the (already evaluated) operands.
    */
   public static Object applyArithmetic(ArithmeticOperator op, Object left, Object right) {
      if (left == null || right == null) {
         return null;
      }
      if (op == ArithmeticOperator.ADD && (left instanceof String || right instanceof String)) {
         return String.valueOf(left).concat(String.valueOf(right));
      }
      if (!(left instanceof Number ln) || !(right instanceof Number rn)) {
         throw new IllegalArgumentException("Cannot apply operator '" + op.getSymbol()
               + "' to operands of type " + className(left) + " and " + className(right));
      }
      if (isIntegral(ln) && isIntegral(rn)) {
         long l = ln.longValue();
         long r = rn.longValue();
         return switch (op) {
            case ADD -> l + r;
            case SUBTRACT -> l - r;
            case MULTIPLY -> l * r;
            case DIVIDE -> divideLong(l, r);
            case MODULO -> moduloLong(l, r);
         };
      }
      double l = ln.doubleValue();
      double r = rn.doubleValue();
      return switch (op) {
         case ADD -> l + r;
         case SUBTRACT -> l - r;
         case MULTIPLY -> l * r;
         case DIVIDE -> divideDouble(l, r);
         case MODULO -> l % r;
      };
   }

   /**
    * Applies a unary minus to the (already evaluated) operand.
    */
   public static Object applyNegate(Object value) {
      if (value == null) {
         return null;
      }
      if (!(value instanceof Number n)) {
         throw new IllegalArgumentException("Cannot negate a value of type " + className(value));
      }
      if (isIntegral(n)) {
         return -(long) n.longValue();
      }
      return -n.doubleValue();
   }

   private static long divideLong(long left, long right) {
      if (right == 0) {
         throw new ArithmeticException("Division by zero");
      }
      return left / right;
   }

   private static double divideDouble(double left, double right) {
      if (right == 0.0d) {
         throw new ArithmeticException("Division by zero");
      }
      return left / right;
   }

   private static long moduloLong(long left, long right) {
      if (right == 0) {
         throw new ArithmeticException("Division by zero");
      }
      return left % right;
   }

   static boolean isIntegral(Number n) {
      return n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte;
   }

   static String className(Object value) {
      return value.getClass().getSimpleName();
   }
}
