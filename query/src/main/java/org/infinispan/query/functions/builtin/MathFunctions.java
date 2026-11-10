package org.infinispan.query.functions.builtin;

import static org.infinispan.query.functions.builtin.FunctionArgs.isIntegral;
import static org.infinispan.query.functions.builtin.FunctionArgs.requireArity;
import static org.infinispan.query.functions.builtin.FunctionArgs.singleNumber;

import java.util.List;

import org.infinispan.api.query.functions.QueryFunction;
import org.infinispan.api.query.functions.QueryFunctionContext;
import org.infinispan.api.query.functions.QueryFunctionSignature;

public final class MathFunctions {

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
            throw new IllegalArgumentException("Function power expects numeric arguments");
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
         throw new IllegalArgumentException("Function " + name + " expects numeric arguments");
      }
      if (isIntegral(na) && isIntegral(nb)) {
         return min ? Math.min(na.longValue(), nb.longValue()) : Math.max(na.longValue(), nb.longValue());
      }
      return min ? Math.min(na.doubleValue(), nb.doubleValue()) : Math.max(na.doubleValue(), nb.doubleValue());
   }
}
