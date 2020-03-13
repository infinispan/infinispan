package org.infinispan.commons.util;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * A collection that defines an iterator method that returns a {@link CloseableIterator}
 * instead of a non closeable one.  This is needed so that iterators can be properly cleaned up.  All other
 * methods will internally clean up any iterators created and don't have other side effects.
 *
 * @author wburns
 * @since 7.0
 */
public interface CloseableIteratorCollection<E> extends Collection<E> {
   /**
    * {@inheritDoc}
    * <p>
    * This iterator should be explicitly closed when iteration upon it is completed. Failure to do so could cause
    * resources to not be freed properly
    */
   @Override
   CloseableIterator<E> iterator();

   /**
    * {@inheritDoc}
    * <p>
    * This spliterator should be explicitly closed after it has been used. Failure to do so could cause
    * resources to not be freed properly
    */
   @Override
   CloseableSpliterator<E> spliterator();

   /**
    * {@inheritDoc}
    * <p>
    * This stream should be explicitly closed after it has been used. Failure to do so could cause
    * resources to not be freed properly
    */
   @Override
   default Stream<E> stream() {
      return Collection.super.stream();
   }

   /**
    * {@inheritDoc}
    * <p>
    * This stream should be explicitly closed after it has been used. Failure to do so could cause
    * resources to not be freed properly
    */
   @Override
   default Stream<E> parallelStream() {
      return Collection.super.parallelStream();
   }
}
