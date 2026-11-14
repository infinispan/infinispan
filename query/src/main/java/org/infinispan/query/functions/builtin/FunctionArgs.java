package org.infinispan.query.functions.builtin;

import java.util.List;

final class FunctionArgs {

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
      throw new IllegalArgumentException("Function " + name + " expects a numeric argument but got " + a.getClass().getSimpleName());
   }

   static Object singleValue(String name, List<Object> args) {
      requireArity(name, args, 1);
      return args.get(0);
   }

   static void requireArity(String name, List<Object> args, int expected) {
      if (args.size() != expected) {
         throw new IllegalArgumentException("Function " + name + " expects " + expected + " argument(s) but got " + args.size());
      }
   }

   static void requireArity(String name, List<Object> args, int min, int max) {
      if (args.size() < min || args.size() > max) {
         throw new IllegalArgumentException("Function " + name + " expects between " + min + " and " + max + " argument(s) but got " + args.size());
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
            throw new IllegalArgumentException("Function " + name + " argument " + (argIndex + 1) + " must be a number, got '" + s + "'");
         }
      }
      throw new IllegalArgumentException("Function " + name + " argument " + (argIndex + 1) + " must be a number, got " + value.getClass().getSimpleName());
   }

   static int toInt(String name, Object value, int argIndex) {
      return (int) toLong(name, value, argIndex);
   }

   static boolean isIntegral(Number n) {
      return n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte;
   }
}
