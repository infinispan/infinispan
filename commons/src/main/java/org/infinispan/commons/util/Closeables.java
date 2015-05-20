package org.infinispan.commons.util;

import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * This class consists exclusively of static methods that operate on or return closeable interfaces.  This is helpful
 * when wanting to change a given interface to an appropriate closeable interface.
 * @since 8.0
 */
public class Closeables {
   private Closeables() { }

   /**
    * Takes a provided closeable iterator and wraps it appropriately so it can be used as a closeable spliterator that
    * will close the iterator when the spliterator is closed.
    * @param iterator The closeable iterator to change to a spliterator
    * @param size The approximate size of the iterator.  If no size is known {@link Long#MAX_VALUE} should be passed
    * @param characteristics The characteristics of the given spliterator as defined on the {@link Spliterator}
    *                        interface
    * @param <E> The type that is the same between the iterator and spliterator
    * @return A spliterator that when closed will close the provided iterator
    */
   public static <E> CloseableSpliterator<E> spliterator(CloseableIterator<? extends E> iterator, long size,
                                                  int characteristics) {
      return new CloseableIteratorAsCloseableSpliterator<>(iterator, size, characteristics);
   }

   /**
    * Creates a closeable spliterator from the given spliterator that does nothing when close is called.
    * @param spliterator The spliterator to wrap to allow it to become a closeable spliterator.
    * @param <T> The type of the spliterators
    * @return A spliterator that does nothing when closed
    */
   public static <T> CloseableSpliterator<T> spliterator(Spliterator<T> spliterator) {
      return new SpliteratorAsCloseableSpliterator<>(spliterator);
   }

   /**
    * Creates a closeable spliterator that when closed will close the underlying stream as well
    * @param stream The stream to change into a closeable spliterator
    * @param <R> The type of the stream
    * @return A spliterator that when closed will also close the underlying stream
    */
   public static <R> CloseableSpliterator<R> spliterator(Stream<R> stream) {
      return new StreamToCloseableSpliterator<>(stream);
   }

   /**
    * Creates a closeable iterator that when closed will close the underlying stream as well
    * @param stream The stream to change into a closeable iterator
    * @param <R> The type of the stream
    * @return An iterator that when closed will also close the underlying stream
    */
   public static <R> CloseableIterator<R> iterator(Stream<? extends R> stream) {
      return new StreamToCloseableIterator<>(stream);
   }

   /**
    * Creates a closeable iterator from the given iterator that does nothing when close is called.
    * @param iterator The iterator to wrap to allow it to become a closeable iterator
    * @param <E> The type of the iterators
    * @return An iterator that does nothing when closed
    */
   public static <E> CloseableIterator<E> iterator(Iterator<? extends E> iterator) {
      return new IteratorAsCloseableIterator<>(iterator);
   }

   private static class IteratorAsCloseableIterator<E> implements CloseableIterator<E> {
      private final Iterator<? extends E> iterator;

      public IteratorAsCloseableIterator(Iterator<? extends E> iterator) {
         this.iterator = iterator;
      }

      @Override
      public void close() {
         // This does nothing
      }

      @Override
      public boolean hasNext() {
         return iterator.hasNext();
      }

      @Override
      public E next() {
         return iterator.next();
      }

      @Override
      public void remove() {
         iterator.remove();
      }
   }

   private static class SpliteratorAsCloseableSpliterator<T> implements CloseableSpliterator<T> {
      private final Spliterator<T> spliterator;

      public SpliteratorAsCloseableSpliterator(Spliterator<T> spliterator) {
         this.spliterator = spliterator;
      }

      @Override
      public void close() {

      }

      @Override
      public boolean tryAdvance(Consumer<? super T> action) {
         return spliterator.tryAdvance(action);
      }

      @Override
      public Spliterator<T> trySplit() {
         return spliterator.trySplit();
      }

      @Override
      public long estimateSize() {
         return spliterator.estimateSize();
      }

      @Override
      public int characteristics() {
         return spliterator.characteristics();
      }
   }

   private static class CloseableIteratorAsCloseableSpliterator<E> extends SpliteratorAsCloseableSpliterator<E> {
      private final CloseableIterator<? extends E> iterator;

      CloseableIteratorAsCloseableSpliterator(CloseableIterator<? extends E> iterator, long size, int characteristics) {
         super(Spliterators.spliterator(iterator, size, characteristics));
         this.iterator = iterator;
      }

      @Override
      public void close() {
         this.iterator.close();
      }
   }

   private static class StreamToCloseableIterator<E> extends IteratorAsCloseableIterator<E> {
      private final Stream<? extends E> stream;

      public StreamToCloseableIterator(Stream<? extends E> stream) {
         super(stream.iterator());
         this.stream = stream;
      }

      @Override
      public void close() {
         stream.close();
      }
   }

   private static class StreamToCloseableSpliterator<T> extends SpliteratorAsCloseableSpliterator<T> {
      private final Stream<T> stream;

      public StreamToCloseableSpliterator(Stream<T> stream) {
         super(stream.spliterator());
         this.stream = stream;
      }

      @Override
      public void close() {
         stream.close();
      }
   }
}
