package org.infinispan.query.functions;

import java.util.List;

/**
 * A named function usable in statement value expressions. This is the
 * extension point for user-defined functions: implementations can be registered
 * in a {@link org.infinispan.query.objectfilter.impl.syntax.update.QueryFunctionRegistry} without any change to the grammar, the
 * value AST or the evaluator.
 *
 * @since 16.4
 */
@FunctionalInterface
public interface QueryFunction {

   /**
    * Evaluates the function on the (already evaluated) arguments.
    *
    * @param args the evaluated argument values, possibly containing {@code null}. The runtime
    *             types of these values depend on the field types they were read from and on the
    *             results of other evaluations (e.g. nested function calls), so implementations
    *             should handle the types they expect defensively.
    * @param ctx  the evaluation context
    * @return the function result, possibly {@code null}
    */
   Object evaluate(List<Object> args, QueryFunctionContext ctx);
}
