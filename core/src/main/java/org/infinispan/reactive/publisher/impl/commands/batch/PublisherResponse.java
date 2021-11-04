package org.infinispan.reactive.publisher.impl.commands.batch;

import java.util.Collections;
import java.util.List;
import java.util.function.ObjIntConsumer;

import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.reactive.publisher.impl.PublisherHandler;

/**
 * The response for a cache publisher request to a given node. It contains an array with how many results there were,
 * which segments were completed or lost during processing, whether the operation has sent all values (complete), and
 * also an offset into the results array of which elements don't map to any of the completed segments. Note that
 * the results will never contain values for a segment that was lost in the same response.
 */
public class PublisherResponse {
   final Object[] results;
   // The completed segments after this request - This may be null
   final IntSet completedSegments;
   // The segments that were lost mid processing - This may be null
   final IntSet lostSegments;
   // How many elements are in the results
   // Note that after being deserialized this is always equal to results.length - local this will be how many entries
   // are in the array
   final int size;
   final boolean complete;
   final List<PublisherHandler.SegmentResult> segmentResults;

   public PublisherResponse(Object[] results, IntSet completedSegments, IntSet lostSegments, int size, boolean complete,
         List<PublisherHandler.SegmentResult> segmentResults) {
      this.results = results;
      this.completedSegments = completedSegments;
      this.lostSegments = lostSegments;
      this.size = size;
      this.complete = complete;
      this.segmentResults = segmentResults;
   }

   public static PublisherResponse emptyResponse(IntSet completedSegments, IntSet lostSegments) {
      return new PublisherResponse(Util.EMPTY_OBJECT_ARRAY, completedSegments, lostSegments, 0, true, Collections.emptyList());
   }

   public Object[] getResults() {
      return results;
   }

   public IntSet getCompletedSegments() {
      return completedSegments;
   }

   public IntSet getLostSegments() {
      return lostSegments;
   }

   public int getSize() {
      return size;
   }

   public boolean isComplete() {
      return complete;
   }

   public List<PublisherHandler.SegmentResult> getSegmentResults() {
      return segmentResults;
   }

   public void keysForNonCompletedSegments(ObjIntConsumer consumer) {
      int segmentResultSize = segmentResults.size();
      if (segmentResultSize == 0) {
         return;
      }
      // The last segment results has ones that weren't completed
      PublisherHandler.SegmentResult segmentResult = segmentResults.get(segmentResultSize - 1);
      int segment = segmentResult.getSegment();
      for (int i = segmentResult.getEntryCount(); i > 0; --i) {
         consumer.accept(results[size - i], segment);
      }
   }

   @Override
   public String toString() {
      return "PublisherResponse{" +
            "size=" + size +
            ", completedSegments=" + completedSegments +
            ", lostSegments=" + lostSegments +
            ", complete=" + complete +
            ", segmentResults=" + segmentResults +
            '}';
   }
}
