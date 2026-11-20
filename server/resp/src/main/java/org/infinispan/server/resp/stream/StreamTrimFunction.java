package org.infinispan.server.resp.stream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function implementing the {@code XTRIM} semantics: evict the oldest entries according to the trimming strategy.
 *
 * <p>
 * The approximate trimming ({@code ~}) is implemented as an exact trimming with a cap on the number of evicted
 * entries when a {@code LIMIT} is provided. The stream bucket does not have internal nodes, so the trimming is always
 * exact to the threshold.
 * </p>
 *
 * @since 16.4
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_STREAM_TRIM_FUNCTION)
public class StreamTrimFunction implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], StreamBucket>, Long> {

   public static final int TRIM_MAXLEN = 1;

   public static final int TRIM_MINID = 2;

   @ProtoField(1)
   final int trimStrategy;

   @ProtoField(2)
   final long maxlen;

   @ProtoField(3)
   final StreamID minId;

   @ProtoField(4)
   final long limit;

   @ProtoFactory
   public StreamTrimFunction(int trimStrategy, long maxlen, StreamID minId, long limit) {
      this.trimStrategy = trimStrategy;
      this.maxlen = maxlen;
      this.minId = minId;
      this.limit = limit;
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<byte[], StreamBucket> entryView) {
      StreamBucket bucket = entryView.peek().orElse(null);
      if (bucket == null) {
         return 0L;
      }
      long trimmed;
      switch (trimStrategy) {
         case TRIM_MAXLEN:
            trimmed = bucket.trimToMaxlen(maxlen, limit);
            break;
         case TRIM_MINID:
            trimmed = bucket.trimToMinId(minId, limit);
            break;
         default:
            return 0L;
      }
      if (trimmed > 0) {
         entryView.set(bucket);
      }
      return trimmed;
   }
}
