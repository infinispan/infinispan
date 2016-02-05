package org.infinispan.util;

import java.io.Serializable;
import java.util.function.Function;

/**
 * This is a functional interface that is the same as a {@link Function} except that it must also be
 * {@link Serializable}
 *
 * @author wburns
 * @since 8.2
 */
@FunctionalInterface
public interface SerializableFunction<T, R> extends Serializable, Function<T, R> {
}
