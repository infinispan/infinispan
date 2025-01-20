package org.infinispan.statetransfer;

import java.util.ArrayList;
import java.util.Collection;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
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
   @ProtoField(number = 1, defaultValue = "-1")
   final int segmentId;

   /**
    * Indicates to receiver if there are more chunks to come for this segment.
    */
   @ProtoField(number = 2, defaultValue = "false")
   final boolean isLastChunk;

   /**
    * The cache entries. They are all guaranteed to be long to the same segment: segmentId.
    */
   final Collection<InternalCacheEntry<?, ?>> cacheEntries;


   @ProtoFactory
   StateChunk(int segmentId, boolean isLastChunk, MarshallableCollection<InternalCacheEntry<?, ?>> entries) {
      this.segmentId = segmentId;
      this.isLastChunk = isLastChunk;
      this.cacheEntries = MarshallableCollection.unwrap(entries, ArrayList::new);
   }

   public StateChunk(int segmentId, Collection<InternalCacheEntry<?, ?>> cacheEntries, boolean isLastChunk) {
      this.segmentId = segmentId;
      this.cacheEntries = cacheEntries;
      this.isLastChunk = isLastChunk;
   }

   public int getSegmentId() {
      return segmentId;
   }

   public Collection<InternalCacheEntry<?, ?>> getCacheEntries() {
      return cacheEntries;
   }

   public boolean isLastChunk() {
      return isLastChunk;
   }

   @ProtoField(3)
   MarshallableCollection<InternalCacheEntry<?, ?>> getEntries() {
      return MarshallableCollection.create(cacheEntries);
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
