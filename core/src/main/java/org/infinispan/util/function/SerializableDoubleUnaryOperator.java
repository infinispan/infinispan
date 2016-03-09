package org.infinispan.util.function;

import java.io.Serializable;
import java.util.function.DoubleUnaryOperator;

/**
 * This is a functional interface that is the same as a {@link DoubleUnaryOperator} except that it must also be
 * {@link java.io.Serializable}
 *
 * @author wburns
 * @since 9.0
 */
public interface SerializableDoubleUnaryOperator extends Serializable, DoubleUnaryOperator {
}
