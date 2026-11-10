package org.infinispan.query.functions.builtin;

import static org.infinispan.query.functions.builtin.FunctionArgs.requireArity;

import java.util.List;

import org.infinispan.query.functions.QueryFunction;
import org.infinispan.query.functions.QueryFunctionContext;
import org.infinispan.query.functions.QueryFunctionSignature;

public final class ConversionFunctions {

   private ConversionFunctions() {
   }

   public static final class ToLong implements QueryFunction {
      @QueryFunctionSignature(args = Object.class, returns = Long.class, category = "conversion")
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         requireArity("toLong", args, 1);
         Object v = args.get(0);
         if (v == null) {
            return null;
         }
         if (v instanceof Number n) {
            return n.longValue();
         }
         if (v instanceof String s) {
            try {
               return Long.parseLong(s.trim());
            } catch (NumberFormatException e) {
               throw new IllegalArgumentException("toLong: cannot parse '" + s + "' as a long");
            }
         }
         throw new IllegalArgumentException("toLong: cannot convert value of type " + v.getClass().getSimpleName());
      }
   }

   public static final class ToDouble implements QueryFunction {
      @QueryFunctionSignature(args = Object.class, returns = Double.class, category = "conversion")
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         requireArity("toDouble", args, 1);
         Object v = args.get(0);
         if (v == null) {
            return null;
         }
         if (v instanceof Number n) {
            return n.doubleValue();
         }
         if (v instanceof String s) {
            try {
               return Double.parseDouble(s.trim());
            } catch (NumberFormatException e) {
               throw new IllegalArgumentException("toDouble: cannot parse '" + s + "' as a double");
            }
         }
         throw new IllegalArgumentException("toDouble: cannot convert value of type " + v.getClass().getSimpleName());
      }
   }

   public static final class ToString implements QueryFunction {
      @QueryFunctionSignature(args = Object.class, returns = String.class, category = "conversion")
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         requireArity("toString", args, 1);
         Object v = args.get(0);
         return v == null ? null : String.valueOf(v);
      }
   }
}
