package org.infinispan.iteration.impl;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Interface describing operations required to properly retrieve values for iteration.
 *
 * @author wburns
 * @since 7.0
 */
public interface EntryRetriever<K, V> {
   /**
    * This method is intended to be ran remotely on a node who has segments that the values have been requested.
    * @param identifier The unique identifier of the iteration request
    * @param origin The node that sent the iteration request
    * @param segments The segments this node wants
    * @param filter The filter to be applied to determine if a value should be used
    * @param converter The converter to run on the values retrieved before returning
    * @param <C> The resulting type of the Converter
    */
   public <C> void startRetrievingValues(UUID identifier, Address origin, Set<Integer> segments,
                                        KeyValueFilter<? super K, ? super V> filter,
                                        Converter<? super K, ? super V, C> converter);

   /**
    * This method is invoked on the local node who started the iteration process for each batch of values.  When
    * either {@code completedSegments} or {@code inDoubtSegments} is not empty (not both could be as well) then the
    * iteration process on this node is complete.
    * @param identifier The unique identifier of the iteration request
    * @param origin The node where the response came from
    * @param completedSegments Which segments have been completed
    * @param inDoubtSegments Which segments are now in doubt due to a rehash
    * @param entries The entries retrieved
    * @param <C> The type of entries values sent back
    */
   public <C> void receiveResponse(UUID identifier, Address origin, Set<Integer> completedSegments,
                                   Set<Integer> inDoubtSegments, Collection<CacheEntry<K, C>> entries);

   /**
    * This is invoked locally on the node that requested the iteration process.  This method will return immediately
    * with the iterator and will process the request asynchronously making more values available as they are received.
    * @param filter An optional filter that will be ran on each key/value to determine if it should be returned.
    * @param converter An optional converter that will be ran on each key/value that will be returned to transform
    *                  the value to a different value if desired
    * @param listener An optional segment listener that can be used to tell the invoker when segments and the iteration
    *                 process is completed
    * @param <C> The type of the resulting values from the converter
    * @return An iterator that should be closed when done working it it, especially if not fully iterated over
    */
   public <C> CloseableIterator<CacheEntry<K, C>> retrieveEntries(KeyValueFilter<? super K, ? super V> filter,
                                                       Converter<? super K, ? super V, ? extends C> converter,
                                                       SegmentListener listener);

   /**
    * This interface describes the call back methods that are invoked when an iteration process completes segments
    * and finally completes.
    */
   public interface SegmentListener {
      /**
       * Notifies the listener that the segment has been completed.  This is only invoked when a segment no longer has
       * any keys left to iterate on.  Thus empty segments will be completed right away, however segments with keys
       * will be completed immediately following the iterator returning the last value for that segment.
       * @param segment The segment that just completed
       * @param sentLastEntry Whether the segment just saw the last value from the iterator.  If this is false then
       *                      the segment was empty
       */
      public void segmentTransferred(int segment, boolean sentLastEntry);
   }
}
