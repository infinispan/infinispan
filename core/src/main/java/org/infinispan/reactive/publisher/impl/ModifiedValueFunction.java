package org.infinispan.reactive.publisher.impl;

import java.util.function.Function;

/**
 * This is a special interface that can mark a Function so that a user can know if the actual value will change
 * when the function is applied or not. This way a function can communicate if it is only observing values or modifying
 * them to the invoker. This can allow the invoker in some cases to optimize a given code path knowing that the values
 * are unchanged.
 * <p>
 * It should be noted that "changed" can be different in a given context. For example if the underlying implementation
 * utilized something similar to a {@link org.infinispan.cache.impl.EncodingFunction} the result could be a different
 * object completely but is essentially the same.
 * @param <I> the input type
 * @param <O> the output type
 */
public interface ModifiedValueFunction<I, O> extends Function<I, O> {
   /**
    * This method should return true when this function changes the actual values of the Publisher. This
    * can be useful for some optimizations that may need to track produced values from the original.
    * @return if the values in the publisher are changed
    */
   boolean isModified();
}
