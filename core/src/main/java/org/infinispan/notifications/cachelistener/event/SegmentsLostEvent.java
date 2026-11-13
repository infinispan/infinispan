package org.infinispan.notifications.cachelistener.event;

import org.infinispan.commons.util.IntSet;
import org.infinispan.notifications.cachelistener.annotation.SegmentsLost;

/**
 * The event passed in to methods annotated with {@link SegmentsLost}.
 *
 * @since 16.4
 */
public interface SegmentsLostEvent<K, V> extends Event<K, V> {

   /**
    * @return The segments that lost all their owners when the new topology was installed. The returned set is never
    *       empty and cannot be modified.
    */
   IntSet getSegments();

   /**
    * @return The id of the topology in which the segments were found to have no owner left.
    */
   int getNewTopologyId();
}
