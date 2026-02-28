package org.infinispan.conflict.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Represents the hash summary of all entries within a single cache segment.
 * The hash is computed as the XOR of individual entry hashes, making it
 * order-independent. Two nodes with identical segment contents will produce
 * the same SegmentHash.
 *
 * @param segmentId  the segment identifier
 * @param hash       XOR-combined hash of all entry hashes in the segment
 * @param entryCount number of entries in the segment
 */
@Proto
@ProtoTypeId(ProtoStreamTypeIds.SEGMENT_HASH)
public record SegmentHash(int segmentId, long hash, int entryCount) {

   /**
    * Returns {@code true} if this hash matches another, meaning both the
    * XOR hash and entry count agree.
    */
   public boolean matches(SegmentHash other) {
      return other != null && this.hash == other.hash && this.entryCount == other.entryCount;
   }
}
