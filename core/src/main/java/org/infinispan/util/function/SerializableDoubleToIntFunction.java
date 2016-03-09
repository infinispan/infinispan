package org.infinispan.util.function;

import java.io.Serializable;
import java.util.function.DoubleFunction;
import java.util.function.DoubleToIntFunction;

/**
 * This is a functional interface that is the same as a {@link DoubleToIntFunction} except that it must also be
 * {@link Serializable}
 *
 * @author wburns
 * @since 9.0
 */
public interface SerializableDoubleToIntFunction extends Serializable, DoubleToIntFunction {
}
