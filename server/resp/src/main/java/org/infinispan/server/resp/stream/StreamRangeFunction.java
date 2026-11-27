package org.infinispan.server.resp.stream;

import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function implementing the range retrieval utilized by {@code XRANGE}, {@code XREVRANGE} and the non-blocking
 * {@code XREAD}: retrieves the entries within the provided ID bounds.
 *
 * @since 16.4
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_STREAM_RANGE_FUNCTION)
public class StreamRangeFunction implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], StreamBucket>, List<StreamEntry>> {

   @ProtoField(1)
   final StreamID start;

   @ProtoField(2)
   final boolean startExclusive;

   @ProtoField(3)
   final StreamID end;

   @ProtoField(4)
   final boolean endExclusive;

   @ProtoField(5)
   final boolean reverse;

   @ProtoField(6)
   final long count;

   @ProtoFactory
   public StreamRangeFunction(StreamID start, boolean startExclusive, StreamID end, boolean endExclusive,
         boolean reverse, long count) {
      this.start = start;
      this.startExclusive = startExclusive;
      this.end = end;
      this.endExclusive = endExclusive;
      this.reverse = reverse;
      this.count = count;
   }

   @Override
   public List<StreamEntry> apply(EntryView.ReadWriteEntryView<byte[], StreamBucket> entryView) {
      StreamBucket bucket = entryView.peek().orElse(null);
      if (bucket == null) {
         return List.of();
      }
      return bucket.range(start, startExclusive, end, endExclusive, reverse, count);
   }
}
