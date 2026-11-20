package org.infinispan.server.resp.stream;

import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A single stream entry: its {@link StreamID} and a flat list of field-value pairs.
 *
 * <p>
 * The field-value pairs are stored as a flat list to preserve the insertion order and duplicates, as Redis guarantees
 * the fields are returned in the same order they were provided to {@code XADD}.
 * </p>
 *
 * @since 16.4
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_STREAM_ENTRY)
public class StreamEntry {

   private final StreamID id;
   private final List<byte[]> fields;

   @ProtoFactory
   public StreamEntry(StreamID id, List<byte[]> fields) {
      this.id = id;
      this.fields = fields;
   }

   @ProtoField(1)
   public StreamID id() {
      return id;
   }

   @ProtoField(2)
   public List<byte[]> fields() {
      return fields;
   }

   /**
    * Estimates the size, in bytes, of this entry when serialized in a RESP reply. Account for the ID and the
    * field-value pairs contents.
    *
    * @return The estimated size in bytes.
    */
   public long replySize() {
      // ID: "ms-seq" plus the bulk string overhead.
      long size = id.toString().length() + 5 + 2;
      for (byte[] field : fields) {
         // Bulk string overhead: '$' + digits + CRLF + contents + CRLF.
         size += field.length + 5 + 2;
      }
      return size;
   }
}
