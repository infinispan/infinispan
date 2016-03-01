package org.infinispan.util.function;

import java.io.Serializable;
import java.util.function.ToIntFunction;

/**
 * This is a functional interface that is the same as a {@link ToIntFunction} except that it must also be
 * {@link Serializable}
 *
 * @author wburns
 * @since 9.0
 */
public interface SerializableToIntFunction<T> extends Serializable, ToIntFunction<T> {
}
