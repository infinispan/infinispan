package org.infinispan.query.functions.builtin;

import static org.infinispan.query.functions.builtin.FunctionArgs.isIntegral;
import static org.infinispan.query.functions.builtin.FunctionArgs.requireArity;
import static org.infinispan.query.functions.builtin.FunctionArgs.singleNumber;

import java.lang.invoke.MethodHandles;
import java.util.List;

import org.infinispan.query.functions.QueryFunction;
import org.infinispan.query.functions.QueryFunctionContext;
import org.infinispan.query.functions.QueryFunctionSignature;
import org.infinispan.query.objectfilter.impl.logging.Log;
import org.jboss.logging.Logger;

public final class MathFunctions {

   private static final Log log = Logger.getMessageLogger(MethodHandles.lookup(), Log.class,
         MathFunctions.class.getName());

   private MathFunctions() {
   }

   public static final class Abs implements QueryFunction {
      @QueryFunctionSignature(args = Number.class, returns = Number.class, category = QueryFunctionSignature.MATH)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         Number n = singleNumber("abs", args);
         if (n == null) {
            return null;
         }
         if (isIntegral(n)) {
            return Math.abs(n.longValue());
         }
         return Math.abs(n.doubleValue());
      }
   }

   public static final class Round implements QueryFunction {
      @QueryFunctionSignature(args = Number.class, returns = Number.class, category = QueryFunctionSignature.MATH)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         Number n = singleNumber("round", args);
         if (n == null) {
            return null;
         }
         return isIntegral(n) ? n.longValue() : Math.round(n.doubleValue());
      }
   }

   public static final class Floor implements QueryFunction {
      @QueryFunctionSignature(args = Number.class, returns = Number.class, category = QueryFunctionSignature.MATH)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         Number n = singleNumber("floor", args);
         if (n == null) {
            return null;
         }
         if (isIntegral(n)) {
            return n.longValue();
         }
         return Math.floor(n.doubleValue());
      }
   }

   public static final class Ceil implements QueryFunction {
      @QueryFunctionSignature(args = Number.class, returns = Number.class, category = QueryFunctionSignature.MATH)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         Number n = singleNumber("ceil", args);
         if (n == null) {
            return null;
         }
         if (isIntegral(n)) {
            return n.longValue();
         }
         return Math.ceil(n.doubleValue());
      }
   }

   public static final class Min implements QueryFunction {
      @QueryFunctionSignature(args = {Number.class, Number.class}, returns = Number.class, category = QueryFunctionSignature.MATH)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         return minMax("min", args, true);
      }
   }

   public static final class Max implements QueryFunction {
      @QueryFunctionSignature(args = {Number.class, Number.class}, returns = Number.class, category = QueryFunctionSignature.MATH)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         return minMax("max", args, false);
      }
   }

   /**
    * Divides one number by another, with the exact semantics of the {@code /}
    * operator: truncating {@code long} math when both operands are integral,
    * {@code double} math otherwise, null propagation and an error on a zero
    * divisor. Being a function call rather than an operator, it is not subject
    * to the lexer conflict between {@code /} and regular expression literals,
    * so it is how an expression performs more than one division.
    */
   public static final class Divide implements QueryFunction {
      @QueryFunctionSignature(args = {Number.class, Number.class}, returns = Number.class, category = QueryFunctionSignature.MATH)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         requireArity("divide", args, 2);
         Object a = args.get(0);
         Object b = args.get(1);
         if (a == null || b == null) {
            return null;
         }
         if (!(a instanceof Number na) || !(b instanceof Number nb)) {
            throw log.functionNumericArgsException("divide");
         }
         if (isIntegral(na) && isIntegral(nb)) {
            long dividend = na.longValue();
            long divisor = nb.longValue();
            if (divisor == 0) {
               throw log.divisionByZeroException();
            }
            return dividend / divisor;
         }
         double dividend = na.doubleValue();
         double divisor = nb.doubleValue();
         if (divisor == 0.0d) {
            throw log.divisionByZeroException();
         }
         return dividend / divisor;
      }
   }

   public static final class Power implements QueryFunction {
      @QueryFunctionSignature(args = {Number.class, Number.class}, returns = Number.class, category = QueryFunctionSignature.MATH)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         requireArity("power", args, 2);
         Object a = args.get(0);
         Object b = args.get(1);
         if (a == null || b == null) {
            return null;
         }
         if (!(a instanceof Number na) || !(b instanceof Number nb)) {
            throw log.functionNumericArgsException("power");
         }
         if (isIntegral(na) && isIntegral(nb)) {
            return (long) Math.pow(na.longValue(), nb.longValue());
         }
         return Math.pow(na.doubleValue(), nb.doubleValue());
      }
   }

   public static final class Sqrt implements QueryFunction {
      @QueryFunctionSignature(args = Number.class, returns = Double.class, category = QueryFunctionSignature.MATH)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         Number n = singleNumber("sqrt", args);
         return n == null ? null : Math.sqrt(n.doubleValue());
      }
   }

   private static Object minMax(String name, List<Object> args, boolean min) {
      requireArity(name, args, 2);
      Object a = args.get(0);
      Object b = args.get(1);
      if (a == null || b == null) {
         return null;
      }
      if (!(a instanceof Number na) || !(b instanceof Number nb)) {
         throw log.functionNumericArgsException(name);
      }
      if (isIntegral(na) && isIntegral(nb)) {
         return min ? Math.min(na.longValue(), nb.longValue()) : Math.max(na.longValue(), nb.longValue());
      }
      return min ? Math.min(na.doubleValue(), nb.doubleValue()) : Math.max(na.doubleValue(), nb.doubleValue());
   }
}
