package org.infinispan.query.functions.builtin;

import java.lang.invoke.MethodHandles;
import java.util.List;

import org.infinispan.query.objectfilter.impl.logging.Log;
import org.jboss.logging.Logger;

final class FunctionArgs {

   private static final Log log = Logger.getMessageLogger(MethodHandles.lookup(), Log.class,
         FunctionArgs.class.getName());

   private FunctionArgs() {
   }

   static Number singleNumber(String name, List<Object> args) {
      requireArity(name, args, 1);
      Object a = args.get(0);
      if (a == null) {
         return null;
      }
      if (a instanceof Number n) {
         return n;
      }
      throw log.functionNumericArgException(name, a.getClass().getSimpleName());
   }

   static Object singleValue(String name, List<Object> args) {
      requireArity(name, args, 1);
      return args.get(0);
   }

   static void requireArity(String name, List<Object> args, int expected) {
      if (args.size() != expected) {
         throw log.functionArityException(name, expected, args.size());
      }
   }

   static void requireArity(String name, List<Object> args, int min, int max) {
      if (args.size() < min || args.size() > max) {
         throw log.functionArityRangeException(name, min, max, args.size());
      }
   }

   static long toLong(String name, Object value, int argIndex) {
      if (value instanceof Number n) {
         return n.longValue();
      }
      if (value instanceof String s) {
         try {
            return Long.parseLong(s.trim());
         } catch (NumberFormatException e) {
            throw log.functionArgumentNotNumberException(name, argIndex + 1, s);
         }
      }
      throw log.functionArgumentNotNumberException(name, argIndex + 1, value.getClass().getSimpleName());
   }

   static int toInt(String name, Object value, int argIndex) {
      return (int) toLong(name, value, argIndex);
   }

   static boolean isIntegral(Number n) {
      return n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte;
   }
}
