package org.infinispan.util.function;

import java.io.Serializable;
import java.util.function.ObjIntConsumer;

/**
 * This is a functional interface that is the same as a {@link ObjIntConsumer} except that it must also be
 * {@link Serializable}
 *
 * @author wburns
 * @since 9.0
 */
public interface SerializableObjIntConsumer<T> extends Serializable, ObjIntConsumer<T> {
}
