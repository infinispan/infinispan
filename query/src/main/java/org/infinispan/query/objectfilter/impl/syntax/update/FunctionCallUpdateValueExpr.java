package org.infinispan.query.objectfilter.impl.syntax.update;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.infinispan.query.functions.QueryFunction;
import org.infinispan.query.functions.QueryFunctionContext;
import org.infinispan.query.objectfilter.impl.logging.Log;
import org.infinispan.query.objectfilter.impl.syntax.ConstantValueExpr;
import org.jboss.logging.Logger;

/**
 * A named function call in an UPDATE statement value expression, e.g.
 * {@code abs(score)}. The function is resolved in the
 * {@link QueryFunctionRegistry} at {@link #prepare} time; the arguments may be
 * {@link UpdateValueExpr} trees or {@link ConstantValueExpr.ParamPlaceholder}
 * instances and are evaluated before the function is invoked.
 *
 * @since 16.4
 */
public final class FunctionCallUpdateValueExpr implements UpdateValueExpr {

   private static final Set<String> REGEXP_FUNCTIONS = Set.of(
         "regexp_like", "regexp_replace", "regexp_substr", "regexp_instr");

   private static final Log log = Logger.getMessageLogger(MethodHandles.lookup(), Log.class,
         FunctionCallUpdateValueExpr.class.getName());

   private final String name;
   private final List<Object> args;

   /**
    * The {@link QueryFunction} resolved during {@link #prepare}, so that per-entity
    * evaluation does not repeat the (normalizing) registry lookup. Written once during
    * {@code prepare}, before the expression is shared with the evaluating threads;
    * {@link #evaluate} falls back to a live lookup when this is {@code null}
    * (evaluation without {@code prepare}, or the function was registered later).
    */
   private QueryFunction resolvedFunction;

   public FunctionCallUpdateValueExpr(String name, List<?> args) {
      this.name = name;
      this.args = new ArrayList<>(args);
   }

   public String getName() {
      return name;
   }

   public List<Object> getArgs() {
      return args;
   }

   @Override
   public void prepare(QueryFunctionRegistry registry, Map<String, Pattern> patternCache) {
      resolvedFunction = registry.get(name);
      if (REGEXP_FUNCTIONS.contains(name.toLowerCase(Locale.ROOT)) && args.size() > 1) {
         Object patternArg = args.get(1);
         if (patternArg instanceof String literal) {
            patternCache.computeIfAbsent(literal, Pattern::compile);
         }
      }
      for (Object arg : args) {
         if (arg instanceof UpdateValueExpr expr) {
            expr.prepare(registry, patternCache);
         }
      }
   }

   @Override
   public Object evaluate(QueryFunctionContext ctx) {
      QueryFunction function = resolvedFunction;
      if (function == null) {
         function = ctx.getFunctions().get(name);
         if (function == null) {
            throw log.unknownFunctionException(name);
         }
      }
      // A fresh, exact-capacity argument list per call is deliberate: the same
      // AST node is evaluated concurrently across entries (CQUpdate applies the
      // updates with CompletionStages.performConcurrently), so no buffer can be
      // shared between evaluations, and List.of() is not an option because the
      // QueryFunction contract admits null arguments and a fixed-size list would
      // break functions that mutate their argument list.
      List<Object> evaluatedArgs = new ArrayList<>(args.size());
      for (Object arg : args) {
         evaluatedArgs.add(UpdateValueEvaluator.evaluateOperand(arg, ctx));
      }
      return function.evaluate(evaluatedArgs, ctx);
   }

   @Override
   public void appendQueryString(StringBuilder sb) {
      sb.append(name).append('(');
      for (int i = 0; i < args.size(); i++) {
         if (i > 0) {
            sb.append(", ");
         }
         Object arg = args.get(i);
         if (arg instanceof UpdateValueExpr expr) {
            expr.appendQueryString(sb);
         } else {
            sb.append(arg);
         }
      }
      sb.append(')');
   }
}
