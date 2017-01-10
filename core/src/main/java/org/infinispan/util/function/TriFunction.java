package org.infinispan.util.function;

/**
 * Represents an operation that accepts three input arguments and returns a
 * result.  This is the three-arity specialization of {@link java.util.function.Function}.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #apply(Object, Object, Object)}.
 *
 * @param <T> the type of the first argument to the operation
 * @param <U> the type of the second argument to the operation
 * @param <V> the type of the third argument to the operation
 *
 * @author Dan Berindei
 * @since 9.0
 */
@FunctionalInterface
public interface TriFunction<T, U, V, R> {
   /**
    * Performs this operation on the given arguments.
    *
    * @param t the first input argument
    * @param u the second input argument
    * @param v the third input argument
    */
   R apply(T t, U u, V v);
}
