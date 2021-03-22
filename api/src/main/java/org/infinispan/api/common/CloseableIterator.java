package org.infinispan.api.common;

import java.util.Iterator;

/**
 * Interface that provides semantics of a {@link Iterator} and {@link AutoCloseable} interfaces.  This is useful when
 * you have data that must be iterated on and may hold resources in the underlying implementation that must be closed.
 * <p>Some implementations may close resources automatically when the iterator is finished being iterated on however
 * this is an implementation detail and all callers should call {@link AutoCloseable#close()} method to be sure all
 * resources are freed properly.</p>
 *
 * @author wburns
 * @since 7.0
 */
public interface CloseableIterator<E> extends AutoCloseable, Iterator<E> {
   @Override
   void close();
}
