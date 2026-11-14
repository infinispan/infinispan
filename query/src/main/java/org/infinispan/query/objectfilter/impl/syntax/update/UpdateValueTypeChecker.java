package org.infinispan.query.objectfilter.impl.syntax.update;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.Date;

import org.infinispan.query.objectfilter.impl.logging.Log;
import org.infinispan.query.objectfilter.impl.syntax.ConstantValueExpr;

/**
 * Parse-time type checking for UPDATE statement value expressions.
 * <p>
 * Infers a {@link ValueCategory} for each value expression (constants, property
 * references, arithmetic, negation, function calls) and validates the result
 * against the type of the target property. The tolerance is deliberately
 * conservative: only combinations that the runtime conversion
 * ({@code UpdateQueryHelper.convertValue}) is <em>guaranteed</em> to reject are
 * turned into parse-time errors. Everything else either passes or is deferred
 * to the runtime, so no statement that works today is newly rejected.
 * <p>
 * Pure-constant numeric subtrees are folded so that definite mistakes such as
 * {@code set rating = 7.0 / 2} (a non-integral value for an integer field) or
 * {@code set rating = 1 / 0} are caught at parse time with a precise error.
 *
 * @since 16.4
 */
public final class UpdateValueTypeChecker {

   /**
    * Coarse type categories of a value expression, mirroring the runtime
    * semantics of {@link UpdateValueEvaluator}.
    */
   public enum ValueCategory {
      NULL, INTEGER, DOUBLE, STRING, BOOLEAN, ENUM, TEMPORAL, UNKNOWN
   }

   /**
    * The inferred category of a value expression, together with the folded
    * constant when the expression is a pure constant (or constant arithmetic)
    * that can be evaluated at parse time.
    */
   public record InferredType(ValueCategory category, Number foldedValue) {

      public static InferredType of(ValueCategory category) {
         return new InferredType(category, null);
      }
   }

   /**
    * Resolves a (rhs) property path to the Java type of the property.
    */
   @FunctionalInterface
   public interface PropertyTypeResolver {
      Class<?> resolve(String[] path);
   }

   private UpdateValueTypeChecker() {
   }

   /**
    * Checks that {@code value} can be assigned to a property of type
    * {@code targetType}, throwing a {@link org.infinispan.query.objectfilter.ParsingException} when the runtime
    * conversion is guaranteed to fail.
    *
    * @param value                the (parsed) value, a constant or a {@link UpdateValueExpr}
    * @param targetType           the Java type of the target property
    * @param propertyPath         textual path of the target property, for error messages
    * @param registry             the function registry, to look up declared return types
    * @param propertyTypeResolver resolves a (rhs) property path to its Java type
    * @param log                  the query module logger
    */
   public static void checkAssignable(Object value, Class<?> targetType, String propertyPath,
                                      QueryFunctionRegistry registry,
                                      PropertyTypeResolver propertyTypeResolver, Log log) {
      InferredType t = infer(value, registry, propertyTypeResolver, log);
      ValueCategory c = t.category();
      if (c == ValueCategory.NULL || c == ValueCategory.UNKNOWN || targetType == null) {
         return;
      }
      // String/char targets accept anything: the runtime toString/fromString fallback handles it.
      if (targetType == String.class || targetType == char.class || targetType == Character.class) {
         return;
      }
      switch (c) {
         case BOOLEAN:
            if (targetType == boolean.class || targetType == Boolean.class) {
               return;
            }
            throw log.getIncompatibleUpdateValueException("boolean", propertyPath, targetType);
         case INTEGER:
         case DOUBLE:
            if (isNumericType(targetType)) {
               if (t.foldedValue() != null && isIntegerType(targetType)) {
                  if (!isIntegral(t.foldedValue())) {
                     throw log.getNonIntegralValueException(t.foldedValue(), propertyPath);
                  }
                  if (!IntegerRange.fits(t.foldedValue(), targetType)) {
                     throw log.getValueOutOfRangeException(t.foldedValue(), propertyPath, targetType);
                  }
               }
               return;
            }
            if (targetType == boolean.class || targetType == Boolean.class
                  || targetType.isEnum() || isTemporalClass(targetType)) {
               throw log.getIncompatibleUpdateValueException("number", propertyPath, targetType);
            }
            // Other targets (e.g. String, handled above): defer to the runtime.
            return;
         case STRING:
            // The runtime has no string->temporal conversion; everything else
            // (numbers, booleans, enums) goes through Util.fromString and may succeed.
            if (isTemporalClass(targetType)) {
               throw log.getIncompatibleUpdateValueException("string", propertyPath, targetType);
            }
            return;
         case ENUM:
            if (targetType.isEnum()) {
               return;
            }
            if (isNumericType(targetType) || targetType == boolean.class || targetType == Boolean.class
                  || isTemporalClass(targetType)) {
               throw log.getIncompatibleUpdateValueException("enum", propertyPath, targetType);
            }
            return;
         case TEMPORAL:
            if (isTemporalClass(targetType)) {
               return;
            }
            if (isNumericType(targetType) || targetType == boolean.class || targetType == Boolean.class
                  || targetType.isEnum()) {
               throw log.getIncompatibleUpdateValueException("date/time", propertyPath, targetType);
            }
            return;
      }
   }

   /**
    * Infers the type category of a parsed value.
    */
   public static InferredType infer(Object value, QueryFunctionRegistry registry,
                                    PropertyTypeResolver propertyTypeResolver, Log log) {
      if (value == null) {
         return InferredType.of(ValueCategory.NULL);
      }
      if (value instanceof ConstantValueExpr.ParamPlaceholder) {
         // Parameter values arrive at execution time.
         return InferredType.of(ValueCategory.UNKNOWN);
      }
      if (value instanceof UpdateValueExpr expr) {
         return inferExpr(expr, registry, propertyTypeResolver, log);
      }
      if (value instanceof Number n) {
         return numericConstant(n);
      }
      if (value instanceof String) {
         return InferredType.of(ValueCategory.STRING);
      }
      if (value instanceof Boolean) {
         return InferredType.of(ValueCategory.BOOLEAN);
      }
      if (value instanceof Enum) {
         return InferredType.of(ValueCategory.ENUM);
      }
      if (value instanceof Date || value instanceof Calendar || isTemporalClass(value.getClass())) {
         return InferredType.of(ValueCategory.TEMPORAL);
      }
      return InferredType.of(ValueCategory.UNKNOWN);
   }

   private static InferredType inferExpr(UpdateValueExpr expr, QueryFunctionRegistry registry,
                                         PropertyTypeResolver propertyTypeResolver, Log log) {
      if (expr instanceof PropertyRefUpdateValue ref) {
         Class<?> type = propertyTypeResolver.resolve(ref.getPath());
         return InferredType.of(categoryOf(type));
      }
      if (expr instanceof FunctionCallUpdateValueExpr fc) {
         Class<?> returnType = registry.getDeclaredReturnType(fc.getName());
         if (returnType != null && returnType != Object.class) {
            return InferredType.of(categoryOf(returnType));
         }
         return InferredType.of(ValueCategory.UNKNOWN);
      }
      if (expr instanceof NegateUpdateValueExpr neg) {
         InferredType inner = infer(neg.getOperand(), registry, propertyTypeResolver, log);
         requireNumeric("-", inner, log);
         Number negated = negate(inner.foldedValue());
         if (negated != null) {
            return new InferredType(inner.category(), negated);
         }
         return InferredType.of(inner.category());
      }
      if (expr instanceof BinaryArithmeticUpdateValueExpr bin) {
         InferredType left = infer(bin.getLeft(), registry, propertyTypeResolver, log);
         InferredType right = infer(bin.getRight(), registry, propertyTypeResolver, log);
         return inferBinOp(bin.getOperator(), left, right, log);
      }
      return InferredType.of(ValueCategory.UNKNOWN);
   }

   private static InferredType inferBinOp(ArithmeticOperator op, InferredType left, InferredType right, Log log) {
      // Null propagation, mirroring the evaluator.
      if (left.category() == ValueCategory.NULL || right.category() == ValueCategory.NULL) {
         return InferredType.of(ValueCategory.NULL);
      }
      // '+' with a String operand is string concatenation, mirroring the evaluator.
      if (op == ArithmeticOperator.ADD
            && (left.category() == ValueCategory.STRING || right.category() == ValueCategory.STRING)) {
         return InferredType.of(ValueCategory.STRING);
      }
      requireNumeric(symbol(op), left, log);
      requireNumeric(symbol(op), right, log);
      boolean integerResult = left.category() == ValueCategory.INTEGER
            && right.category() == ValueCategory.INTEGER;
      ValueCategory result = integerResult ? ValueCategory.INTEGER : ValueCategory.DOUBLE;
      Number lv = left.foldedValue();
      Number rv = right.foldedValue();
      if (lv == null || rv == null) {
         return InferredType.of(result);
      }
      // Fold pure-constant numeric expressions.
      boolean bothLong = lv instanceof Long && rv instanceof Long;
      long li = lv.longValue();
      long ri = rv.longValue();
      return switch (op) {
         case ADD -> bothLong ? new InferredType(ValueCategory.INTEGER, li + ri)
               : new InferredType(ValueCategory.DOUBLE, lv.doubleValue() + rv.doubleValue());
         case SUBTRACT -> bothLong ? new InferredType(ValueCategory.INTEGER, li - ri)
               : new InferredType(ValueCategory.DOUBLE, lv.doubleValue() - rv.doubleValue());
         case MULTIPLY -> bothLong ? new InferredType(ValueCategory.INTEGER, li * ri)
               : new InferredType(ValueCategory.DOUBLE, lv.doubleValue() * rv.doubleValue());
         case DIVIDE -> {
            if (isZero(rv)) {
               throw log.getConstantDivisionByZeroException();
            }
            yield bothLong ? new InferredType(ValueCategory.INTEGER, li / ri)
                  : new InferredType(ValueCategory.DOUBLE, lv.doubleValue() / rv.doubleValue());
         }
         case MODULO -> {
            if (isZero(rv)) {
               throw log.getConstantDivisionByZeroException();
            }
            yield bothLong ? new InferredType(ValueCategory.INTEGER, li % ri)
                  : new InferredType(ValueCategory.DOUBLE, lv.doubleValue() % rv.doubleValue());
         }
      };
   }

   private static boolean isZero(Number n) {
      return n instanceof Long ? n.longValue() == 0 : n.doubleValue() == 0.0d;
   }

   private static void requireNumeric(String opSymbol, InferredType t, Log log) {
      if (t.category() != ValueCategory.INTEGER && t.category() != ValueCategory.DOUBLE
            && t.category() != ValueCategory.UNKNOWN) {
         throw log.getArithmeticRequiresNumericOperandsException(opSymbol);
      }
   }

   private static Number negate(Number n) {
      if (n instanceof Long l) {
         return -l;
      }
      if (n instanceof Double d) {
         return -d;
      }
      return null;
   }

   private static InferredType numericConstant(Number n) {
      if (n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte) {
         return new InferredType(ValueCategory.INTEGER, n.longValue());
      }
      return new InferredType(ValueCategory.DOUBLE, n.doubleValue());
   }

   static ValueCategory categoryOf(Class<?> type) {
      if (type == null) {
         return ValueCategory.UNKNOWN;
      }
      if (type == int.class || type == Integer.class || type == long.class || type == Long.class
            || type == short.class || type == Short.class || type == byte.class || type == Byte.class) {
         return ValueCategory.INTEGER;
      }
      if (type == float.class || type == Float.class || type == double.class || type == Double.class) {
         return ValueCategory.DOUBLE;
      }
      if (type == String.class || type == char.class || type == Character.class) {
         return ValueCategory.STRING;
      }
      if (type == boolean.class || type == Boolean.class) {
         return ValueCategory.BOOLEAN;
      }
      if (type.isEnum()) {
         return ValueCategory.ENUM;
      }
      if (isTemporalClass(type)) {
         return ValueCategory.TEMPORAL;
      }
      if (type == BigDecimal.class || type == BigInteger.class) {
         return ValueCategory.DOUBLE;
      }
      return ValueCategory.UNKNOWN;
   }

   static boolean isTemporalClass(Class<?> type) {
      return type == Date.class || type == java.sql.Date.class || type == java.sql.Timestamp.class
            || Calendar.class.isAssignableFrom(type) || type.getName().startsWith("java.time.");
   }

   static boolean isNumericType(Class<?> type) {
      return type == int.class || type == Integer.class || type == long.class || type == Long.class
            || type == short.class || type == Short.class || type == byte.class || type == Byte.class
            || type == float.class || type == Float.class || type == double.class || type == Double.class
            || type == BigDecimal.class || type == BigInteger.class;
   }

   static boolean isIntegerType(Class<?> type) {
      return type == int.class || type == Integer.class || type == long.class || type == Long.class
            || type == short.class || type == Short.class || type == byte.class || type == Byte.class;
   }


   private static boolean isIntegral(Number n) {
      double dv = n.doubleValue();
      return !Double.isNaN(dv) && !Double.isInfinite(dv) && dv == Math.floor(dv);
   }

   private static String symbol(ArithmeticOperator op) {
      return switch (op) {
         case ADD -> "+";
         case SUBTRACT -> "-";
         case MULTIPLY -> "*";
         case DIVIDE -> "/";
         case MODULO -> "%";
      };
   }
}
