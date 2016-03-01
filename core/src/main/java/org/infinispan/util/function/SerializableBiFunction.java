package org.infinispan.util.function;

import java.io.Serializable;
import java.util.function.BiFunction;

/**
 * This is a functional interface that is the same as a {@link BiFunction} except that it must also be
 * {@link Serializable}
 *
 * @author wburns
 * @since 9.0
 */
public interface SerializableBiFunction<T, U, R> extends Serializable, BiFunction<T, U, R> {
}
