package org.infinispan.commons.util;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.BaseStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
      if (spliterator instanceof CloseableSpliterator) {
         return (CloseableSpliterator<T>) spliterator;
      }
      return new SpliteratorAsCloseableSpliterator<>(spliterator);
   }

   /**
    * Creates a closeable spliterator that when closed will close the underlying stream as well
    * @param stream The stream to change into a closeable spliterator
    * @param <R> The type of the stream
    * @return A spliterator that when closed will also close the underlying stream
    */
   public static <R> CloseableSpliterator<R> spliterator(BaseStream<R, Stream<R>> stream) {
      Spliterator<R> spliterator = stream.spliterator();
      if (spliterator instanceof CloseableSpliterator) {
         return (CloseableSpliterator<R>) spliterator;
      }
      return new StreamToCloseableSpliterator<>(stream, spliterator);
   }

   /**
    * Creates a closeable iterator that when closed will close the underlying stream as well
    * @param stream The stream to change into a closeable iterator
    * @param <R> The type of the stream
    * @return An iterator that when closed will also close the underlying stream
    */
   public static <R> CloseableIterator<R> iterator(BaseStream<R, Stream<R>> stream) {
      Iterator<R> iterator = stream.iterator();
      if (iterator instanceof CloseableIterator) {
         return (CloseableIterator<R>) iterator;
      }
      return new StreamToCloseableIterator<>(stream, iterator);
   }

   /**
    * Creates a closeable iterator from the given iterator that does nothing when close is called.
    * @param iterator The iterator to wrap to allow it to become a closeable iterator
    * @param <E> The type of the iterators
    * @return An iterator that does nothing when closed
    */
   public static <E> CloseableIterator<E> iterator(Iterator<? extends E> iterator) {
      if (iterator instanceof CloseableIterator) {
         return (CloseableIterator<E>) iterator;
      }
      return new IteratorAsCloseableIterator<>(iterator);
   }

   /**
    * Creates a stream that when closed will also close the underlying spliterator
    * @param spliterator spliterator to back the stream and subsequently close
    * @param parallel whether or not the returned stream is parallel or not
    * @param <E> the type of the stream
    * @return the stream to use
    */
   public static <E> Stream<E> stream(CloseableSpliterator<E> spliterator, boolean parallel) {
      Stream<E> stream = StreamSupport.stream(spliterator, parallel);
      stream.onClose(spliterator::close);
      return stream;
   }

   /**
    * Creates a stream that when closed will also close the underlying iterator
    * @param iterator iterator to back the stream and subsequently close
    * @param parallel whether or not the returned stream is parallel or not
    * @param size the size of the iterator if known, otherwise {@link Long#MAX_VALUE} should be passed.
    * @param characteristics the characteristics of the iterator to be used
    * @param <E> the type of the stream
    * @return the stream to use
    */
   public static <E> Stream<E> stream(CloseableIterator<E> iterator, boolean parallel, long size, int characteristics) {
      Stream<E> stream = StreamSupport.stream(Spliterators.spliterator(iterator, size, characteristics), parallel);
      stream.onClose(iterator::close);
      return stream;
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
      private final BaseStream<E, Stream<E>> stream;

      public StreamToCloseableIterator(BaseStream<E, Stream<E>> stream, Iterator<E> iterator) {
         super(iterator);
         this.stream = stream;
      }

      @Override
      public void close() {
         stream.close();
      }
   }

   private static class StreamToCloseableSpliterator<T> extends SpliteratorAsCloseableSpliterator<T> {
      private final BaseStream<T, Stream<T>> stream;

      public StreamToCloseableSpliterator(BaseStream<T, Stream<T>> stream, Spliterator<T> spliterator) {
         super(spliterator);
         this.stream = stream;
      }

      @Override
      public void close() {
         stream.close();
      }
   }
}
