package org.infinispan.scattered;

import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.topology.CacheTopology;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public interface ScatteredVersionManager<K> {
   long incrementVersion(int segment);

   /**
    * We have to use version here, too, because otherwise if a key is committed on primary
    * but not confirmed to be backed up yet, and there's an older invalidation scheduled,
    * we would invalidate with the new version and could lose the old backup
    */
   void scheduleKeyInvalidation(K key, long version, boolean removal);

   void clearInvalidations();

   boolean startFlush();

   void registerSegment(int segment);

   void unregisterSegment(int segment);

   void setSegmentVersion(int segment, long version);

   boolean isVersionActual(int segment, long version);

   /**
    * All key + version data from given segment have been received.
    * @param segment
    * @param expectValues
    */
   void keyTransferFinished(int segment, boolean expectValues);

   CompletableFuture<long[]> computeMaxVersions(CacheTopology cacheTopology);

   SegmentState getSegmentState(int segment);

   void setValuesTransferTopology(int topologyId);

   void valuesReceived(int topologyId);

   void waitForValues(int topologyId);

   void setOwnedSegments(Set<Integer> segments);

   void setNonOwnedSegments(Set<Integer> segments);

   enum SegmentState {
      NOT_OWNED('N'),      // Not owned and request to new version ends with failure
      BLOCKED('B'),        // Owned but the highest timestamp is unknown
      KEY_TRANSFER('K'),   // Owned but does not have previous value
      VALUE_TRANSFER('V'), // Knows who has the correct value but does not know the value itself
      OWNED('O')           // Owned and operating
      ;

      private final char singleLetter;

      SegmentState(char singleLetter) {
         this.singleLetter = singleLetter;
      }

      public char singleLetter() {
         return singleLetter;
      }
   }
}
