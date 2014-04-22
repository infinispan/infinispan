package org.infinispan.commons.util;

import java.io.Closeable;

/**
 * Interface that provides semantics of a {@link Iterable} and {@link java.io.Closeable} interfaces.  This is
 * useful when you have data that must be iterated on and may hold resources in the underlying implementation that
 * must be closed.
 * <p>The close method will close any existing iterators that may be open to free resources</p>
 *
 * @author wburns
 * @since 7.0
 */
public interface CloseableIterable<E> extends Closeable, Iterable<E> {
}
