package org.infinispan.query.objectfilter.impl.syntax.update;

import java.util.Map;
import java.util.regex.Pattern;

import org.infinispan.query.functions.QueryFunctionContext;

/**
 * Root of the AST for Ickle value expressions: the right-hand sides of the
 * {@code SET}/{@code ADD}/{@code REMOVE} operations of an {@code UPDATE}
 * statement, and the computed projections of a {@code SELECT} clause.
 * <p>
 * This is a standalone hierarchy that is not related to the predicate
 * ({@code WHERE}) expression tree, so the predicate visitor machinery is left untouched.
 * Nodes are small, immutable, plain Java objects; they never cross the wire,
 * as queries are reparsed on the node that executes them.
 *
 * @since 16.4
 */
public interface UpdateValueExpr {

   /**
    * Evaluates this expression against the given context.
    *
    * @param ctx the evaluation context holding the target entity and the named parameters
    * @return the evaluated value, possibly {@code null}
    */
   Object evaluate(QueryFunctionContext ctx);

   /**
    * Prepares this expression for repeated evaluation, e.g. pre-compiling
    * regular-expression patterns that appear as literals. Called once before
    * the per-entity evaluation loop.
    *
    * @param registry     the function registry (used to check whether a name is a regexp function)
    * @param patternCache shared cache for compiled patterns; literal patterns are pre-compiled here
    */
   default void prepare(QueryFunctionRegistry registry, Map<String, Pattern> patternCache) {
   }

   /**
    * Appends a string representation of this expression to the given builder.
    *
    * @param sb the string builder to append to
    */
   void appendQueryString(StringBuilder sb);
}
