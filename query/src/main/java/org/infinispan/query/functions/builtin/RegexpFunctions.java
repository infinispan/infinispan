package org.infinispan.query.functions.builtin;

import static org.infinispan.query.functions.builtin.FunctionArgs.requireArity;

import java.util.List;
import java.util.regex.Matcher;

import org.infinispan.api.query.functions.QueryFunction;
import org.infinispan.api.query.functions.QueryFunctionContext;
import org.infinispan.api.query.functions.QueryFunctionSignature;

public final class RegexpFunctions {

   public static final String REGEXP = "regexp";

   private RegexpFunctions() {
   }

   public static final class RegexpLike implements QueryFunction {
      @QueryFunctionSignature(args = {Object.class, String.class}, returns = Boolean.class, category = REGEXP)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         requireArity("regexp_like", args, 2);
         Object s = args.get(0);
         Object pattern = args.get(1);
         if (s == null || pattern == null) {
            return null;
         }
         return ctx.compilePattern(String.valueOf(pattern)).matcher(String.valueOf(s)).find();
      }
   }

   public static final class RegexpReplace implements QueryFunction {
      @QueryFunctionSignature(args = {Object.class, String.class, Object.class}, returns = String.class, category = REGEXP)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         requireArity("regexp_replace", args, 3);
         Object s = args.get(0);
         Object pattern = args.get(1);
         Object replacement = args.get(2);
         if (s == null || pattern == null || replacement == null) {
            return null;
         }
         return ctx.compilePattern(String.valueOf(pattern)).matcher(String.valueOf(s)).replaceAll(String.valueOf(replacement));
      }
   }

   public static final class RegexpSubstr implements QueryFunction {
      @QueryFunctionSignature(args = {Object.class, String.class}, returns = String.class, category = REGEXP)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         requireArity("regexp_substr", args, 2);
         Object s = args.get(0);
         Object pattern = args.get(1);
         if (s == null || pattern == null) {
            return null;
         }
         Matcher m = ctx.compilePattern(String.valueOf(pattern)).matcher(String.valueOf(s));
         return m.find() ? m.group() : null;
      }
   }

   public static final class RegexpInstr implements QueryFunction {
      @QueryFunctionSignature(args = {Object.class, String.class}, returns = Integer.class, category = REGEXP)
      @Override
      public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
         requireArity("regexp_instr", args, 2);
         Object s = args.get(0);
         Object pattern = args.get(1);
         if (s == null || pattern == null) {
            return null;
         }
         Matcher m = ctx.compilePattern(String.valueOf(pattern)).matcher(String.valueOf(s));
         return m.find() ? m.start() + 1 : 0;
      }
   }
}
