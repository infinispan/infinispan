package org.infinispan.conflict.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;

/**
 * Computes an order-independent hash summary for cache segments.
 * <p>
 * For each entry, the key and value are marshalled to bytes, then hashed with
 * MurmurHash3_x64_64. The per-entry hash is {@code hash(keyBytes) XOR hash(valueBytes)}.
 * All per-entry hashes are XOR'd together to produce the segment hash.
 * XOR is commutative and associative, so the result is independent of iteration order.
 */
public class SegmentHasher {

   public static final int DEFAULT_BUCKET_COUNT = 32;
   private static final int HASH_SEED = 9001;

   private final InternalDataContainer<?, ?> dataContainer;
   private final Marshaller marshaller;

   public SegmentHasher(InternalDataContainer<?, ?> dataContainer, Marshaller marshaller) {
      this.dataContainer = dataContainer;
      this.marshaller = marshaller;
   }

   /**
    * Computes the hash for a single segment.
    */
   public SegmentHash computeHash(int segmentId) {
      long xorHash = 0;
      int count = 0;
      Iterator<InternalCacheEntry<?, ?>> it = cast(dataContainer.iterator(IntSets.immutableSet(segmentId)));
      while (it.hasNext()) {
         InternalCacheEntry<?, ?> entry = it.next();
         xorHash ^= hashEntry(entry);
         count++;
      }
      return new SegmentHash(segmentId, xorHash, count);
   }

   /**
    * Computes per-bucket hashes for all entries in a segment.
    */
   public List<BucketHash> computeBucketHashes(int segmentId, int bucketCount) {
      long[] hashes = new long[bucketCount];
      int[] counts = new int[bucketCount];
      Iterator<InternalCacheEntry<?, ?>> it = cast(dataContainer.iterator(IntSets.immutableSet(segmentId)));
      while (it.hasNext()) {
         InternalCacheEntry<?, ?> entry = it.next();
         int bucket = bucketForKey(entry.getKey(), bucketCount);
         hashes[bucket] ^= hashEntry(entry);
         counts[bucket]++;
      }
      List<BucketHash> result = new ArrayList<>(bucketCount);
      for (int b = 0; b < bucketCount; b++) {
         result.add(new BucketHash(segmentId, b, hashes[b], counts[b]));
      }
      return result;
   }

   /**
    * Computes per-bucket hashes for multiple segments.
    */
   public List<BucketHash> computeAllBucketHashes(IntSet segments, int bucketCount) {
      List<BucketHash> result = new ArrayList<>(segments.size() * bucketCount);
      segments.forEach((int seg) -> result.addAll(computeBucketHashes(seg, bucketCount)));
      return result;
   }

   /**
    * Derives the segment-level hash from pre-computed bucket hashes.
    * Since XOR is associative, the segment hash equals the XOR of all bucket hashes,
    * and the entry count is the sum of all bucket counts.
    * This avoids a separate iteration over the data container.
    */
   public static SegmentHash deriveSegmentHash(int segmentId, List<BucketHash> bucketHashes) {
      long segmentHash = 0;
      int entryCount = 0;
      for (BucketHash bh : bucketHashes) {
         segmentHash ^= bh.hash();
         entryCount += bh.entryCount();
      }
      return new SegmentHash(segmentId, segmentHash, entryCount);
   }

   /**
    * Determines which bucket a key belongs to.
    */
   public int bucketForKey(Object key, int bucketCount) {
      try {
         byte[] keyBytes = marshaller.objectToByteBuffer(key);
         long keyHash = MurmurHash3.MurmurHash3_x64_64(keyBytes, HASH_SEED);
         return (int) (keyHash & (bucketCount - 1));
      } catch (IOException | InterruptedException e) {
         if (e instanceof InterruptedException) Thread.currentThread().interrupt();
         throw new RuntimeException("Failed to marshal key for bucket assignment", e);
      }
   }

   private long hashEntry(InternalCacheEntry<?, ?> entry) {
      try {
         byte[] keyBytes = marshaller.objectToByteBuffer(entry.getKey());
         byte[] valueBytes = marshaller.objectToByteBuffer(entry.getValue());
         return MurmurHash3.MurmurHash3_x64_64(keyBytes, HASH_SEED)
               ^ MurmurHash3.MurmurHash3_x64_64(valueBytes, HASH_SEED);
      } catch (IOException | InterruptedException e) {
         if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
         }
         throw new RuntimeException("Failed to marshal entry for segment hashing", e);
      }
   }

   @SuppressWarnings("unchecked")
   private static Iterator<InternalCacheEntry<?, ?>> cast(Iterator<? extends InternalCacheEntry<?, ?>> it) {
      return (Iterator<InternalCacheEntry<?, ?>>) (Iterator<?>) it;
   }
}
