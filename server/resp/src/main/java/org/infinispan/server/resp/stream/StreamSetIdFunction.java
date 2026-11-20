package org.infinispan.server.resp.stream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.resp.exception.RespCommandException;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function implementing the {@code XSETID} semantics: set the last generated ID, and optionally the entries-added
 * counter and the maximal deleted entry ID, of an existing stream.
 *
 * <p>
 * Following the Redis semantics, the new ID is validated against the maximal deleted entry ID and, when the stream
 * has entries, against the top entry ID. The last generated ID is not part of the validation, so an empty stream
 * accepts any ID.
 * </p>
 *
 * @since 16.4
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_STREAM_SET_ID_FUNCTION)
public class StreamSetIdFunction implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], StreamBucket>, Boolean> {

   @ProtoField(1)
   final StreamID id;

   @ProtoField(2)
   final boolean entriesAddedGiven;

   @ProtoField(3)
   final long entriesAdded;

   @ProtoField(4)
   final boolean maxDeletedGiven;

   @ProtoField(5)
   final StreamID maxDeletedId;

   @ProtoFactory
   public StreamSetIdFunction(StreamID id, boolean entriesAddedGiven, long entriesAdded, boolean maxDeletedGiven,
         StreamID maxDeletedId) {
      this.id = id;
      this.entriesAddedGiven = entriesAddedGiven;
      this.entriesAdded = entriesAdded;
      this.maxDeletedGiven = maxDeletedGiven;
      this.maxDeletedId = maxDeletedId;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<byte[], StreamBucket> entryView) {
      StreamBucket bucket = entryView.peek().orElse(null);
      if (bucket == null) {
         throw new RespCommandException("ERR no such key");
      }

      if (maxDeletedGiven && id.compareTo(maxDeletedId) < 0) {
         throw new RespCommandException("ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id");
      }
      if (id.compareTo(bucket.maxDeletedId()) < 0) {
         throw new RespCommandException("ERR The ID specified in XSETID is smaller than current max_deleted_entry_id");
      }
      // When the stream has at least one entry, the new ID must be equal or greater than the current top entry,
      // otherwise the ID monotonicity assumption is violated.
      StreamEntry top = bucket.lastEntry();
      if (top != null && id.compareTo(top.id()) < 0) {
         throw new RespCommandException("ERR The ID specified in XSETID is smaller than the target stream top item");
      }
      if (entriesAddedGiven && bucket.size() > entriesAdded) {
         throw new RespCommandException("ERR The entries_added specified in XSETID is smaller than the target stream length");
      }

      bucket.setLastId(id);
      if (entriesAddedGiven) {
         bucket.setEntriesAdded(entriesAdded);
      }
      if (maxDeletedGiven && !maxDeletedId.isZero()) {
         bucket.setMaxDeletedId(maxDeletedId);
      }
      entryView.set(bucket);
      return Boolean.TRUE;
   }
}
