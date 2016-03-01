package org.infinispan.util.function;

import java.io.Serializable;
import java.util.function.BiConsumer;

/**
 * This is a functional interface that is the same as a {@link BiConsumer} except that it must also be
 * {@link Serializable}
 *
 * @author wburns
 * @since 9.0
 */
public interface SerializableBiConsumer<T, U> extends Serializable, BiConsumer<T, U> {
}
