package org.infinispan.query.functions.builtin;

import static org.infinispan.query.functions.builtin.FunctionArgs.requireArity;
import static org.infinispan.query.functions.builtin.FunctionArgs.singleValue;
import static org.infinispan.query.functions.builtin.FunctionArgs.toInt;

import java.util.List;
import java.util.Locale;

import org.infinispan.query.functions.QueryFunction;
import org.infinispan.query.functions.QueryFunctionContext;
import org.infinispan.query.functions.QueryFunctionSignature;

public final class StringFunctions {

   private StringFunctions() {
   }

   public static final class Concat implements QueryFunction {
      @QueryFunctionSignature(args = Object.class, returns = String.class, category = QueryFunctionSignature.STRING)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         requireArity("concat", args, 1, Integer.MAX_VALUE);
         StringBuilder sb = new StringBuilder();
         for (Object arg : args) {
            if (arg == null) {
               return null;
            }
            sb.append(arg);
         }
         return sb.toString();
      }
   }

   public static final class Upper implements QueryFunction {
      @QueryFunctionSignature(args = Object.class, returns = String.class, category = QueryFunctionSignature.STRING)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         Object s = singleValue("upper", args);
         return s == null ? null : String.valueOf(s).toUpperCase(Locale.ROOT);
      }
   }

   public static final class Lower implements QueryFunction {
      @QueryFunctionSignature(args = Object.class, returns = String.class, category = QueryFunctionSignature.STRING)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         Object s = singleValue("lower", args);
         return s == null ? null : String.valueOf(s).toLowerCase(Locale.ROOT);
      }
   }

   public static final class Trim implements QueryFunction {
      @QueryFunctionSignature(args = Object.class, returns = String.class, category = QueryFunctionSignature.STRING)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         Object s = singleValue("trim", args);
         return s == null ? null : String.valueOf(s).trim();
      }
   }

   public static final class Length implements QueryFunction {
      @QueryFunctionSignature(args = Object.class, returns = Integer.class, category = QueryFunctionSignature.STRING)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         Object s = singleValue("length", args);
         return s == null ? null : String.valueOf(s).length();
      }
   }

   public static final class Substring implements QueryFunction {
      @QueryFunctionSignature(args = {Object.class, Number.class}, returns = String.class, category = QueryFunctionSignature.STRING)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         requireArity("substring", args, 2, 3);
         Object s = args.get(0);
         if (s == null) {
            return null;
         }
         String str = String.valueOf(s);
         int begin = toInt("substring", args.get(1), 1);
         int end = args.size() == 3 ? toInt("substring", args.get(2), 2) : str.length() + 1;
         try {
            return str.substring(begin - 1, end - 1);
         } catch (StringIndexOutOfBoundsException e) {
            throw new IllegalArgumentException("substring: invalid range [" + begin + ", " + end + ") for string of length " + str.length());
         }
      }
   }

   public static final class Replace implements QueryFunction {
      @QueryFunctionSignature(args = {Object.class, Object.class, Object.class}, returns = String.class, category = QueryFunctionSignature.STRING)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         requireArity("replace", args, 3);
         Object s = args.get(0);
         Object from = args.get(1);
         Object to = args.get(2);
         if (s == null || from == null || to == null) {
            return null;
         }
         return String.valueOf(s).replace(String.valueOf(from), String.valueOf(to));
      }
   }
}
