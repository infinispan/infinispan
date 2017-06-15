package org.infinispan.commons.util;

import java.util.function.Function;

/**
 * This is a marker interface to be used with {@link Function} which signals to some implementors that
 * the function returns distinct values.  This can be helpful because when a function is applied to data is ambiguous
 * if the resulting data produced is distinct or not.  This allows some callers of this method to apply additional
 * performance optimizations taking this into account.
 * <p>
 * If a <b>function</b> is implemented with this and it doesn't produce distinct values, the operation of the
 * consumer of this function may be undefined.
 *
 * @author wburns
 * @since 9.0
 */
public interface InjectiveFunction<T, R> extends Function<T, R> {
}
