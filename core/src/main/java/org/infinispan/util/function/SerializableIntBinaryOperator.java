package org.infinispan.util.function;

import java.io.Serializable;
import java.util.function.IntBinaryOperator;

/**
 * This is a functional interface that is the same as a {@link IntBinaryOperator} except that it must also be
 * {@link Serializable}
 *
 * @author wburns
 * @since 9.0
 */
public interface SerializableIntBinaryOperator extends Serializable, IntBinaryOperator {
}
