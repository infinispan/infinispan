package org.infinispan.server.resp.stream;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Bucket used to store the stream data type.
 *
 * <p>
 * The bucket keeps the entries sorted in ascending ID order. IDs are strictly monotonic: {@code XADD} only appends
 * entries with an ID greater than the last recorded one. The bucket also keeps the bookkeeping required by
 * {@code XSETID} and {@code XINFO STREAM}:
 * <ul>
 * <li>{@code lastId}: the last generated (or explicitly set) ID. It survives entry deletions and trimming, so
 * {@code XADD} continues to generate increasing IDs and {@code XREAD} can still detect new entries on a stream that
 * ran dry.</li>
 * <li>{@code entriesAdded}: lifetime counter of entries added, never decremented.</li>
 * <li>{@code maxDeletedId}: the maximum ID removed by {@code XDEL} or {@code XTRIM}.</li>
 * <li>{@code recordedFirstId}: the recorded ID of the first entry in the stream.</li>
 * </ul>
 * Following Redis semantics, the bucket is <b>not</b> removed when it becomes empty.
 * </p>
 *
 * @since 16.4
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_STREAM_BUCKET)
public class StreamBucket {

   private List<StreamEntry> entries;
   private StreamID lastId;
   private long entriesAdded;
   private StreamID maxDeletedId;
   private StreamID recordedFirstId;

   public StreamBucket() {
      this.entries = new java.util.ArrayList<>(0);
      this.lastId = StreamID.ZERO;
      this.entriesAdded = 0;
      this.maxDeletedId = StreamID.ZERO;
      this.recordedFirstId = StreamID.ZERO;
   }

   @ProtoFactory
   StreamBucket(List<StreamEntry> entries, StreamID lastId, long entriesAdded, StreamID maxDeletedId,
         StreamID recordedFirstId) {
      this.entries = entries;
      this.lastId = lastId;
      this.entriesAdded = entriesAdded;
      this.maxDeletedId = maxDeletedId;
      this.recordedFirstId = recordedFirstId;
   }

   @ProtoField(1)
   List<StreamEntry> getEntries() {
      return entries;
   }

   @ProtoField(2)
   StreamID getLastId() {
      return lastId;
   }

   @ProtoField(number = 3, defaultValue = "0")
   long getEntriesAdded() {
      return entriesAdded;
   }

   @ProtoField(4)
   StreamID getMaxDeletedId() {
      return maxDeletedId;
   }

   @ProtoField(5)
   StreamID getRecordedFirstId() {
      return recordedFirstId;
   }

   public StreamID lastId() {
      return lastId;
   }

   public void setLastId(StreamID id) {
      this.lastId = id;
   }

   public long entriesAdded() {
      return entriesAdded;
   }

   public void setEntriesAdded(long entriesAdded) {
      this.entriesAdded = entriesAdded;
   }

   public StreamID maxDeletedId() {
      return maxDeletedId;
   }

   public void setMaxDeletedId(StreamID maxDeletedId) {
      this.maxDeletedId = maxDeletedId;
   }

   public StreamID recordedFirstId() {
      return recordedFirstId;
   }

   public boolean isEmpty() {
      return entries.isEmpty();
   }

   public int size() {
      return entries.size();
   }

   public StreamEntry get(int index) {
      return entries.get(index);
   }

   public StreamEntry firstEntry() {
      return entries.isEmpty() ? null : entries.get(0);
   }

   public StreamEntry lastEntry() {
      return entries.isEmpty() ? null : entries.get(entries.size() - 1);
   }

   /**
    * Appends an entry with the given ID and field-value pairs. The ID must be strictly greater than the current
    * {@link #lastId}; the caller is responsible for the validation.
    *
    * @param id The entry ID.
    * @param fields The flat list of field-value pairs.
    */
   public void append(StreamID id, List<byte[]> fields) {
      if (entries.isEmpty()) {
         recordedFirstId = id;
      }
      entries.add(new StreamEntry(id, fields));
      lastId = id;
      entriesAdded++;
   }

   /**
    * Removes the entries matching any of the provided IDs.
    *
    * @param ids The IDs to remove.
    * @return The number of entries removed.
    */
   public long removeIds(Collection<StreamID> ids) {
      if (ids.isEmpty() || entries.isEmpty()) {
         return 0;
      }

      Set<StreamID> toRemove = new HashSet<>(ids);
      long removed = 0;
      boolean removedAny = false;
      for (var it = entries.iterator(); it.hasNext(); ) {
         StreamEntry entry = it.next();
         if (toRemove.contains(entry.id())) {
            removedAny = true;
            maxDeletedId = max(maxDeletedId, entry.id());
            it.remove();
            removed++;
         }
      }

      if (removedAny) {
         // For a non-empty bucket the recorded first entry is the first live entry. When the bucket runs dry, keep
         // the successor of the previously recorded first entry.
         recordedFirstId = entries.isEmpty() ? recordedFirstId.next() : entries.get(0).id();
      }
      return removed;
   }

   /**
    * Trims the stream evicting the oldest entries while the length exceeds the maximum provided. At most
    * {@code limit} entries are evicted when the limit is non-negative.
    *
    * @param maxlen The maximum length to keep.
    * @param limit The maximum number of entries to evict, or a negative value for no limit.
    * @return The number of entries evicted.
    */
   public long trimToMaxlen(long maxlen, long limit) {
      long excess = entries.size() - maxlen;
      if (excess <= 0) {
         return 0;
      }
      long toEvict = limit < 0 ? excess : Math.min(excess, limit);
      return evictOldest(toEvict);
   }

   /**
    * Trims the stream evicting entries with an ID lower than the provided threshold. At most {@code limit} entries
    * are evicted when the limit is non-negative.
    *
    * @param minId The threshold: entries with an ID lower than this are evicted.
    * @param limit The maximum number of entries to evict, or a negative value for no limit.
    * @return The number of entries evicted.
    */
   public long trimToMinId(StreamID minId, long limit) {
      long toEvict = 0;
      for (StreamEntry entry : entries) {
         if (entry.id().compareTo(minId) < 0) {
            toEvict++;
         } else {
            break;
         }
      }
      if (limit >= 0) {
         toEvict = Math.min(toEvict, limit);
      }
      return evictOldest(toEvict);
   }

   private long evictOldest(long count) {
      if (count <= 0) {
         return 0;
      }
      count = Math.min(count, entries.size());
      StreamID lastEvicted = entries.get((int) (count - 1)).id();
      entries.subList(0, (int) count).clear();
      // Following the Redis semantics, the trimming does not update the maximal deleted entry ID, only XDEL does.
      recordedFirstId = entries.isEmpty() ? lastEvicted.next() : entries.get(0).id();
      return count;
   }

   /**
    * Retrieves the entries in the given ID range, in ascending or descending order.
    *
    * @param start The lower bound of the range.
    * @param startExclusive Whether the lower bound is exclusive.
    * @param end The upper bound of the range.
    * @param endExclusive Whether the upper bound is exclusive.
    * @param reverse Whether to return the entries in descending ID order.
    * @param count The maximum number of entries to return, or a non-positive value to return all the entries.
    * @return The entries in the requested range.
    */
   public List<StreamEntry> range(StreamID start, boolean startExclusive, StreamID end, boolean endExclusive,
         boolean reverse, long count) {
      int from = lowerBound(start, startExclusive);
      int to = upperBound(end, endExclusive);
      List<StreamEntry> result = new java.util.ArrayList<>(Math.max(0, to - from));
      if (!reverse) {
         for (int i = from; i < to; i++) {
            if (count > 0 && result.size() >= count) {
               break;
            }
            result.add(entries.get(i));
         }
      } else {
         for (int i = to - 1; i >= from; i--) {
            if (count > 0 && result.size() >= count) {
               break;
            }
            result.add(entries.get(i));
         }
      }
      return result;
   }

   /**
    * @return The index of the first entry with an ID greater than (or equal to, when not exclusive) the bound.
    */
   private int lowerBound(StreamID bound, boolean exclusive) {
      int low = 0, high = entries.size();
      while (low < high) {
         int mid = (low + high) >>> 1;
         int compare = entries.get(mid).id().compareTo(bound);
         if (compare < 0 || (exclusive && compare == 0)) {
            low = mid + 1;
         } else {
            high = mid;
         }
      }
      return low;
   }

   /**
    * @return The index after the last entry with an ID lower than (or equal to, when not exclusive) the bound.
    */
   private int upperBound(StreamID bound, boolean exclusive) {
      int low = 0, high = entries.size();
      while (low < high) {
         int mid = (low + high) >>> 1;
         int compare = entries.get(mid).id().compareTo(bound);
         if (compare > 0 || (exclusive && compare == 0)) {
            high = mid;
         } else {
            low = mid + 1;
         }
      }
      return low;
   }

   private static StreamID max(StreamID a, StreamID b) {
      return a.compareTo(b) >= 0 ? a : b;
   }
}
