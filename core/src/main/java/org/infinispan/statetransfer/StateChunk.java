package org.infinispan.statetransfer;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Encapsulates a chunk of cache entries that belong to the same segment. This representation is suitable for sending it
 * to another cache during state transfer.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateChunk {

   /**
    * The id of the segment for which we push cache entries.
    */
   private final int segmentId;

   /**
    * The cache entries. They are all guaranteed to be long to the same segment: segmentId.
    */
   private final Collection<InternalCacheEntry> cacheEntries;

   /**
    * Indicates to receiver if there are more chunks to come for this segment.
    */
   private final boolean isLastChunk;

   public StateChunk(int segmentId, Collection<InternalCacheEntry> cacheEntries, boolean isLastChunk) {
      this.segmentId = segmentId;
      this.cacheEntries = cacheEntries;
      this.isLastChunk = isLastChunk;
   }

   public int getSegmentId() {
      return segmentId;
   }

   public Collection<InternalCacheEntry> getCacheEntries() {
      return cacheEntries;
   }

   public boolean isLastChunk() {
      return isLastChunk;
   }

   @Override
   public String toString() {
      return "StateChunk{" +
            "segmentId=" + segmentId +
            ", cacheEntries=" + cacheEntries.size() +
            ", isLastChunk=" + isLastChunk +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<StateChunk> {

      @Override
      public Integer getId() {
         return Ids.STATE_CHUNK;
      }

      @Override
      public Set<Class<? extends StateChunk>> getTypeClasses() {
         return Collections.<Class<? extends StateChunk>>singleton(StateChunk.class);
      }

      @Override
      public void writeObject(ObjectOutput output, StateChunk object) throws IOException {
         output.writeInt(object.segmentId);
         output.writeObject(object.cacheEntries);
         output.writeBoolean(object.isLastChunk);
      }

      @Override
      @SuppressWarnings("unchecked")
      public StateChunk readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int segmentId = input.readInt();
         Collection<InternalCacheEntry> cacheEntries = (Collection<InternalCacheEntry>) input.readObject();
         boolean isLastChunk = input.readBoolean();
         return new StateChunk(segmentId, cacheEntries, isLastChunk);
      }
   }
}
