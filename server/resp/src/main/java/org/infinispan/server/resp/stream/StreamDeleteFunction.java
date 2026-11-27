package org.infinispan.server.resp.stream;

import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function implementing the {@code XDEL} semantics: remove the entries matching the provided IDs.
 *
 * @since 16.4
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_STREAM_DELETE_FUNCTION)
public class StreamDeleteFunction implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], StreamBucket>, Long> {

   @ProtoField(1)
   final List<StreamID> ids;

   @ProtoFactory
   public StreamDeleteFunction(List<StreamID> ids) {
      this.ids = ids;
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<byte[], StreamBucket> entryView) {
      StreamBucket bucket = entryView.peek().orElse(null);
      if (bucket == null) {
         return 0L;
      }
      long removed = bucket.removeIds(ids);
      if (removed > 0) {
         entryView.set(bucket);
      }
      return removed;
   }
}
