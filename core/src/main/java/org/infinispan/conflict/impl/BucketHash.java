package org.infinispan.conflict.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Represents the hash summary of all entries within a single bucket of a cache segment.
 * Each segment is subdivided into buckets based on key hash, and each bucket's hash
 * is computed independently. This allows narrowing conflict detection to specific
 * buckets rather than fetching all entries in a mismatched segment.
 *
 * @param segmentId  the segment identifier
 * @param bucketId   the bucket identifier within the segment
 * @param hash       XOR-combined hash of all entry hashes in this bucket
 * @param entryCount number of entries in this bucket
 */
@Proto
@ProtoTypeId(ProtoStreamTypeIds.BUCKET_HASH)
public record BucketHash(int segmentId, int bucketId, long hash, int entryCount) {

   /**
    * Returns {@code true} if this hash matches another, meaning both the
    * XOR hash and entry count agree.
    */
   public boolean matches(BucketHash other) {
      return other != null && this.hash == other.hash && this.entryCount == other.entryCount;
   }
}
