package org.infinispan.query.functions.builtin;

import static org.infinispan.query.functions.builtin.FunctionArgs.requireArity;

import java.lang.invoke.MethodHandles;
import java.util.List;

import org.infinispan.query.functions.QueryFunction;
import org.infinispan.query.functions.QueryFunctionContext;
import org.infinispan.query.functions.QueryFunctionSignature;
import org.infinispan.query.objectfilter.impl.logging.Log;
import org.jboss.logging.Logger;

public final class ConversionFunctions {

   private static final Log log = Logger.getMessageLogger(MethodHandles.lookup(), Log.class,
         ConversionFunctions.class.getName());

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
               throw log.functionCannotParseNumberException("toLong", s);
            }
         }
         throw log.functionCannotConvertTypeException("toLong", v.getClass().getSimpleName());
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
               throw log.functionCannotParseNumberException("toDouble", s);
            }
         }
         throw log.functionCannotConvertTypeException("toDouble", v.getClass().getSimpleName());
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
