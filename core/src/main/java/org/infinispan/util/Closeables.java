package org.infinispan.util;

import java.util.Iterator;

import org.infinispan.commons.util.CloseableIterator;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;

/**
 * This class is used solely for the purpose of converting classes only in core to corresponding closeable variants.
 * @author wburns
 * @since 9.3
 */
public class Closeables {
   private Closeables() { }

   /**
    * Converts a {@link Publisher} to a {@link CloseableIterator} by utilizing items fetched into an array and
    * refetched as they are consumed from the iterator. The iterator when closed will also close the underlying
    * {@link org.reactivestreams.Subscription} when subscribed to the publisher.
    * @param publisher the publisher to convert
    * @param fetchSize how many entries to hold in memory at once in preparation for the iterators consumption
    * @param <E> value type
    * @return an iterator that when closed will cancel the subscription
    */
   public static <E> CloseableIterator<E> iterator(Publisher<E> publisher, int fetchSize) {
      // This iterator from rxjava2 implements Disposable akin to Closeable
      Iterator<E> iterator = Flowable.fromPublisher(publisher).blockingIterable(fetchSize).iterator();
      return new CloseableIterator<E>() {
         @Override
         public void close() {
            ((Disposable) iterator).dispose();
         }

         @Override
         public boolean hasNext() {
            return iterator.hasNext();
         }

         @Override
         public E next() {
            return iterator.next();
         }
      };
   }
}
