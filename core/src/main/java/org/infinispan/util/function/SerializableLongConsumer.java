package org.infinispan.util.function;

import java.io.Serializable;
import java.util.function.LongConsumer;

/**
 * This is a functional interface that is the same as a {@link LongConsumer} except that it must also be
 * {@link Serializable}
 *
 * @author wburns
 * @since 9.0
 */
public interface SerializableLongConsumer extends Serializable, LongConsumer {
}
