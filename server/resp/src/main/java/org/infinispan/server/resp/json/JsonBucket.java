package org.infinispan.server.resp.json;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.resp.commands.connection.MemoryEntrySizeUtils;

/**
 * Bucket used to store Set data type.
 *
 * @author Vittorio Rigamonti
 * @since 15.0
 */
@Proto
@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_BUCKET)
public record JsonBucket(byte[] value) {
public static long memoryHeaderSize() {
   return MemoryHeader.headerSize;
}

// Only used to get the overhead without hardcoding
private static class MemoryHeader {
   @SuppressWarnings("unused")
   byte[] value;
   static long headerSize = MemoryEntrySizeUtils.calculateSize(new MemoryHeader());
}
}
