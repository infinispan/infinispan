package org.infinispan;

import org.infinispan.commons.util.CloseableIteratorCollection;
import org.infinispan.commons.util.Experimental;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * A collection type that returns special Cache based streams that have additional options to tweak behavior.
 * @param <E> The type of the collection
 * @since 8.0
 */
public interface CacheCollection<E> extends CloseableIteratorCollection<E> {
   @Override
   CacheStream<E> stream();

   @Override
   CacheStream<E> parallelStream();

   /**
    * Returns a publisher that will publish all elements that map to the given segment. Note this publisher may
    * require going remotely to retrieve elements depending on the underlying configuration and flags applied
    * to the original Cache used to create this CacheCollection.
    * @param segment the segment that all published elements belong to
    * @return Publisher that will publish the elements for the given segment
    * @implSpec Default implementation just does:
    * <pre> {@code
    * return localPublisher(org.infinispan.commons.util.IntSets.immutableSet(segment));
    * }</pre>
    */
   @Experimental
   default Publisher<E> localPublisher(int segment) {
      return localPublisher(IntSets.immutableSet(segment));
   }

   /**
    * Returns a publisher that will publish all elements that map to the given segment. Note this publisher may
    * require going remotely to retrieve elements depending on the underlying configuration and flags applied
    * to the original Cache used to create this CacheCollection.
    * @param segments the segments that all published elements belong to
    * @return Publisher that will publish the elements for the given segments
    * @implSpec Default implementation falls back to stream filtering out the given segments
    * <pre> {@code
    * return io.reactivex.Flowable.fromIterable(() -> stream().filterKeySegments(segments).iterator());
    * }</pre>
    */
   @Experimental
   default Publisher<E> localPublisher(IntSet segments) {
      return Flowable.fromIterable(() -> stream().filterKeySegments(segments).iterator());
   }
}
