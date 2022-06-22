package org.infinispan.cli.util;

import java.util.Iterator;
import java.util.function.Function;

/**
 * @since 14.0
 **/
public class TransformingIterator<S, T> implements Iterator<T> {
   private final Iterator<S> iterator;
   private final Function<S, T> transformer;

   public TransformingIterator(Iterator<S> iterator, Function<S, T> transformer) {
      this.iterator = iterator;
      this.transformer = transformer;
   }

   @Override
   public boolean hasNext() {
      return iterator.hasNext();
   }

   @Override
   public T next() {
      return transformer.apply(iterator.next());
   }
}
