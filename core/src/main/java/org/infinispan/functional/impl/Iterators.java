package org.infinispan.functional.impl;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Iterators {

   /**
    * Provide a lazily evaluated closeable iterator for a stream.
    */
   public static <T> CloseableIterator<T> of(Stream<T> stream) {
      return new StreamCloseableIterator<>(stream);
   }

   /**
    * Provide a lazily evaluated closeable spliterator for a stream.
    */
   public static <T> CloseableSpliterator<T> spliterator(Stream<T> stream) {
      return new StreamCloseableSpliterator<>(stream);
   }

   /**
    * Provide an eagerly evaluated closeable iterator for a stream.
    * By eagerly evaluating the stream, it can fully consumed in advance,
    * and the closeable iterator returned is simply an iterator over the
    * already produced streamed results.
    */
   public static <T> CloseableIterator<T> eager(Stream<T> stream) {
      List<T> list = stream.collect(Collectors.toList());
      return new StreamCloseableIterator<>(list.stream());
   }

   private Iterators() {
      // Cannot be instantiated, it's just a holder class
   }

   private static final class StreamCloseableIterator<T> implements CloseableIterator<T> {
      volatile boolean isClosed = false;
      final Iterator<T> it;

      private StreamCloseableIterator(Stream<T> stream) {
         this.it = stream.iterator();
      }

      @Override
      public boolean hasNext() {
         return !isClosed && it.hasNext();
      }

      @Override
      public T next() {
         if (isClosed)
            throw new NoSuchElementException("Iterator closed");

         return it.next();
      }

      @Override
      public void close() {
         isClosed = true;
      }
   }

   private static final class StreamCloseableSpliterator<T> implements CloseableSpliterator<T> {
      private final Spliterator<T> it;

      private StreamCloseableSpliterator(Stream<T> stream) {
         this.it = stream.spliterator();
      }

      @Override
      public void close() {
         // TODO: Customise this generated block
      }

      @Override
      public boolean tryAdvance(Consumer<? super T> action) {
         return false;  // TODO: Customise this generated block
      }

      @Override
      public Spliterator<T> trySplit() {
         return null;  // TODO: Customise this generated block
      }

      @Override
      public long estimateSize() {
         return 0;  // TODO: Customise this generated block
      }

      @Override
      public int characteristics() {
         return 0;  // TODO: Customise this generated block
      }
   }

}
