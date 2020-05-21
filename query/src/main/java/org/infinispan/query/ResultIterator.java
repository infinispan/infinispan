package org.infinispan.query;

import java.io.Closeable;

import org.infinispan.commons.util.CloseableIterator;

/**
 * Iterates over query results. Must be closed when done with it.
 *
 * @author Marko Luksa
 * @deprecated Since 11.0 with no replacement.
 */
@Deprecated
public interface ResultIterator<E> extends CloseableIterator<E>, Closeable {
}
