package org.infinispan.cache.impl;

import java.util.function.UnaryOperator;

import org.infinispan.commons.util.InjectiveFunction;

/**
 * This is a marker interface to signal that this function may perform an encoding of the provided value. The returned
 * value therefore will always be equivalent to the provided value, but may be in a slightly different form (whether
 * due to unwrapping, encoding or transcoding. This may allow certain optimizations knowing that the value is
 * equivalent to what it was before.
 * @author wburns
 * @since 10.1
 */
public interface EncodingFunction<T> extends UnaryOperator<T>, InjectiveFunction<T, T> {
}
