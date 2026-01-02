package org.infinispan.util.function;

import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * Represents a predicate (boolean-valued function) of three arguments.  This is
 * the three-arity specialization of {@link Predicate}.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #test(Object, Object, Object)}.
 *
 * @param <T> the type of the first argument to the predicate
 * @param <U> the type of the second argument the predicate
 * @param <V> the type of the third argument the predicate
 *
 * @see Predicate
 * @see BiPredicate
 * @since 15.0
 */
@FunctionalInterface
public interface TriPredicate<T, U, V> {

   /**
    * Evaluates this predicate on the given arguments.
    *
    * @param t the first input argument
    * @param u the second input argument
    * @param v the third input argument
    * @return {@code true} if the input arguments match the predicate,
    * otherwise {@code false}
    */
   boolean test(T t, U u, V v);
}
