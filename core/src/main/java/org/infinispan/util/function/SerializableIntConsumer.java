package org.infinispan.util.function;

import java.io.Serializable;
import java.util.function.IntConsumer;

/**
 * This is a functional interface that is the same as a {@link IntConsumer} except that it must also be
 * {@link Serializable}
 *
 * @author wburns
 * @since 9.0
 */
public interface SerializableIntConsumer extends Serializable, IntConsumer {
}
