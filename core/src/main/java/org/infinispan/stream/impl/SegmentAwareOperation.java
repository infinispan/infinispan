package org.infinispan.stream.impl;

/**
 * Terminal stream operation that is aware of segments being lost.  This interface describes a single callback method
 * to be invoked on the operation when a segment is lost and it is concurrently running some operation.
 * @since 8.0
 */
public interface SegmentAwareOperation {
   /**
    * This method will be invoked when the operation is known to be performing on a given set of segments
    * and this node no longer owns 1 or many of them.  Returns whether the lost segment affected the results or
    * not.  If stopIfLost is trure then doneWithOperation will not be invoked as it would normally.
    * @param stopIfLost argument to tell the operation that if this segment affects that it should not
    *                   perform any more operations if possible as all segments have been lost.
    * @return whether or not this operation was affected by the loss of segments
    */
   boolean lostSegment(boolean stopIfLost);
}
