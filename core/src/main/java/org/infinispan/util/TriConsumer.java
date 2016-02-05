package org.infinispan.util;

import java.util.function.Consumer;

/**
 * Represents an operation that accepts three input arguments and returns no
 * result.  This is the three-arity specialization of {@link Consumer}.
 * Unlike most other functional interfaces, {@code TriConsumer} is expected
 * to operate via side-effects.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #accept(Object, Object, Object)}.
 *
 * @param <T> the type of the first argument to the operation
 * @param <U> the type of the second argument to the operation
 * @param <V> the type of the third argument to the operation
 *
 * @author wburns
 * @since 8.2
 */
@FunctionalInterface
public interface TriConsumer<T, U, V> {
   /**
    * Performs this operation on the given arguments.
    *
    * @param t the first input argument
    * @param u the second input argument
    * @param v the third input argument
    */
   void accept(T t, U u, V v);
}
