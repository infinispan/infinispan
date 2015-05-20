package org.infinispan.util;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorCollection;
import org.infinispan.commons.util.CloseableSpliterator;

import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Delegating collection that produces closeable iterators and spliterators from the collection returned from
 * {@link AbstractDelegatingCloseableIteratorCollection#delegate()} method.
 * @param <E> The type in the collection
 */
public abstract class AbstractDelegatingCloseableIteratorCollection<E> extends AbstractDelegatingCollection<E>
        implements CloseableIteratorCollection<E> {

   protected abstract CloseableIteratorCollection<E> delegate();

   @Override
   public CloseableIterator<E> iterator() {
      return delegate().iterator();
   }

   @Override
   public CloseableSpliterator<E> spliterator() {
      return delegate().spliterator();
   }
}
