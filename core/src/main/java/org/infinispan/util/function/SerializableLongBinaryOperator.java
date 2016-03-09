package org.infinispan.util.function;

import java.io.Serializable;
import java.util.function.LongBinaryOperator;

/**
 * This is a functional interface that is the same as a {@link LongBinaryOperator} except that it must also be
 * {@link Serializable}
 *
 * @author wburns
 * @since 9.0
 */
public interface SerializableLongBinaryOperator extends Serializable, LongBinaryOperator {
}
