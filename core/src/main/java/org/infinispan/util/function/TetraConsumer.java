package org.infinispan.util.function;


/**
 * Represents an operation that accepts three input arguments and returns no
 * result. This is the four-arity specialization of {@link java.util.function.Consumer}.
 * Unlike most other functional interfaces, {@code TriConsumer} is expected
 * to operate via side-effects.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #accept(Object, Object, Object, Object)}.
 *
 * @param <T> the type of the first argument to the operation
 * @param <U> the type of the second argument to the operation
 * @param <V> the type of the third argument to the operation
 *
 * @author Dan Berindei
 * @since 9.0
 */
@FunctionalInterface
public interface TetraConsumer<T, U, V, W> {
   /**
    * Performs this operation on the given arguments.
    *
    * @param t the first input argument
    * @param u the second input argument
    * @param v the third input argument
    * @param w the fourth input argument
    */
   void accept(T t, U u, V v, W w);
}
