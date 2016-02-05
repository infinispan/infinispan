package org.infinispan.util;

import java.io.Serializable;

/**
 * This is a functional interface that is the same as a {@link Runnable} except that it must also be
 * {@link Serializable}
 *
 * @author wburns
 * @since 8.2
 */
@FunctionalInterface
public interface SerializableRunnable extends Serializable, Runnable {
}
