package org.infinispan.util;

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * This is a functional interface that is the same as a {@link Callable} except that it must also be
 * {@link Serializable}
 *
 * @author wburns
 * @since 8.2
 */
@FunctionalInterface
public interface SerializableCallable<V> extends Serializable, Callable<V> {
}
