package org.infinispan.util;

import org.infinispan.commons.util.CloseableIterator;
import org.reactivestreams.Publisher;

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
    * @deprecated since 11.0 Please use {@link org.infinispan.commons.util.Closeables#iterator(Publisher, int)} instead.
    */
   @Deprecated
   public static <E> CloseableIterator<E> iterator(Publisher<E> publisher, int fetchSize) {
     return org.infinispan.commons.util.Closeables.iterator(publisher, fetchSize);
   }
}
