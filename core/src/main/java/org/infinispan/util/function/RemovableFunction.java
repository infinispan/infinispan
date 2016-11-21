package org.infinispan.util.function;

import java.util.function.Function;

/**
 * Placeholder interface used to describe a function that can be used as is for removal.  This is useful for
 * allowing an iterator to use removal.
 * <p>
 * Users shouldn't ever need to use this interface directly
 * @author wburns
 * @since 9.0
 */
public interface RemovableFunction<T, R> extends Function<T, R> {
}
