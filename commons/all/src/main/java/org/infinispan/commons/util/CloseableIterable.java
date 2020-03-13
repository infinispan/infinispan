package org.infinispan.commons.util;

/**
 * Interface that provides semantics of a {@link Iterable} and {@link AutoCloseable} interfaces.  This is
 * useful when you have data that must be iterated on and may hold resources in the underlying implementation that
 * must be closed.
 * <p>The close method will close any existing iterators that may be open to free resources</p>
 *
 * @author wburns
 * @since 7.0
 */
public interface CloseableIterable<E> extends AutoCloseable, Iterable<E> {
   @Override
   void close();

   @Override
   CloseableIterator<E> iterator();
}
