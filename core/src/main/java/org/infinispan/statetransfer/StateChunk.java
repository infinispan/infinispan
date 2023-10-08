package org.infinispan.statetransfer;

import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.protostream.impl.MarshallableList;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Encapsulates a chunk of cache entries that belong to the same segment. This representation is suitable for sending it
 * to another cache during state transfer.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@ProtoTypeId(ProtoStreamTypeIds.STATE_CHUNK)
public class StateChunk {

   /**
    * The id of the segment for which we push cache entries.
    */
   @ProtoField(1)
   final int segmentId;

   /**
    * Indicates to receiver if there are more chunks to come for this segment.
    */
   @ProtoField(2)
   final boolean isLastChunk;

   /**
    * The cache entries. They are all guaranteed to be long to the same segment: segmentId.
    */
   final List<InternalCacheEntry<?, ?>> cacheEntries;


   @ProtoFactory
   StateChunk(int segmentId, boolean isLastChunk, MarshallableList<InternalCacheEntry<?, ?>> entries) {
      this.segmentId = segmentId;
      this.isLastChunk = isLastChunk;
      this.cacheEntries = MarshallableList.unwrap(entries);
   }

   public StateChunk(int segmentId, List<InternalCacheEntry<?, ?>> cacheEntries, boolean isLastChunk) {
      this.segmentId = segmentId;
      this.cacheEntries = cacheEntries;
      this.isLastChunk = isLastChunk;
   }

   public int getSegmentId() {
      return segmentId;
   }

   public List<InternalCacheEntry<?, ?>> getCacheEntries() {
      return cacheEntries;
   }

   public boolean isLastChunk() {
      return isLastChunk;
   }

   @ProtoField(3)
   MarshallableList<InternalCacheEntry<?, ?>> getEntries() {
      return MarshallableList.create(cacheEntries);
   }

   @Override
   public String toString() {
      return "StateChunk{" +
            "segmentId=" + segmentId +
            ", cacheEntries=" + cacheEntries.size() +
            ", isLastChunk=" + isLastChunk +
            '}';
   }
}
