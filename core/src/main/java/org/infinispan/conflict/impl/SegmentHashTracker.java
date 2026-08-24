package org.infinispan.conflict.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.impl.EvictionManagerImpl;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

@Scope(Scopes.NAMED_CACHE)
public class SegmentHashTracker {

   @Inject Configuration configuration;
   @Inject @ComponentName(KnownComponentNames.INTERNAL_MARSHALLER) Marshaller marshaller;
   @Inject EvictionManager<?, ?> evictionManager;

   private AtomicLongArray[] bucketHashes;
   private AtomicIntegerArray[] bucketCounts;
   private int bucketCount;
   private boolean enabled;
   private boolean hasStores;
   // Incremented before each resetSegment call. getBucketHashes() checks this before and after
   // reading a segment; if the value changed a concurrent reset raced the read and the snapshot
   // is unreliable — the caller falls back to a full segment fetch.
   private final AtomicInteger resetVersion = new AtomicInteger();

   @Start
   public void start() {
      hasStores = configuration.persistence().usingStores();
      enabled = configuration.clustering().cacheMode().isClustered()
            && configuration.clustering().partitionHandling().resolveConflictsOnMerge()
            && configuration.persistence().stores().stream().noneMatch(StoreConfiguration::shared);

      if (!enabled) return;

      if (evictionManager instanceof EvictionManagerImpl<?, ?> emi) {
         emi.setSegmentHashTracker(this);
      }

      int numSegments = configuration.clustering().hash().numSegments();
      bucketCount = configuration.clustering().partitionHandling().hashBuckets();

      bucketHashes = new AtomicLongArray[numSegments];
      bucketCounts = new AtomicIntegerArray[numSegments];
      for (int i = 0; i < numSegments; i++) {
         bucketHashes[i] = new AtomicLongArray(bucketCount);
         bucketCounts[i] = new AtomicIntegerArray(bucketCount);
      }
   }

   public boolean isEnabled() {
      return enabled;
   }

   public boolean hasStores() {
      return hasStores;
   }

   public int bucketCount() {
      return bucketCount;
   }

   public Marshaller marshaller() {
      return marshaller;
   }

   public SegmentHasher.HashAndBucket computeHashAndBucket(Object key, Object value) {
      return SegmentHasher.computeHashAndBucket(key, value, bucketCount, marshaller);
   }

   /**
    * Computes the entry hash for {@code value} given a pre-computed {@code keyHash}, avoiding
    * a second marshal of the key. The bucket is derived from {@code keyHash} directly.
    * The entry hash is {@code keyHash ^ hashObject(value)}.
    */
   public SegmentHasher.HashAndBucket computeHashAndBucket(long keyHash, Object value) {
      int bucket = (int) (keyHash & (bucketCount - 1));
      long entryHash = keyHash ^ SegmentHasher.hashObject(value, marshaller);
      return new SegmentHasher.HashAndBucket(entryHash, bucket);
   }

   public int computeBucketIndex(Object key) {
      return SegmentHasher.computeBucket(key, bucketCount, marshaller);
   }

   public void recordInsert(int segment, int bucket, long hash) {
      bucketHashes[segment].getAndAccumulate(bucket, hash, (prev, h) -> prev ^ h);
      bucketCounts[segment].getAndIncrement(bucket);
   }

   public void recordRemove(int segment, int bucket, long hash) {
      bucketHashes[segment].getAndAccumulate(bucket, hash, (prev, h) -> prev ^ h);
      bucketCounts[segment].getAndDecrement(bucket);
   }

   public void recordUpdate(int segment, int bucket, long oldHash, long newHash) {
      long delta = oldHash ^ newHash;
      bucketHashes[segment].getAndAccumulate(bucket, delta, (prev, d) -> prev ^ d);
   }

   public void resetSegment(int segment) {
      // Increment version before zeroing so any concurrent getBucketHashes() that began
      // before this reset will detect the change and discard its partial snapshot.
      resetVersion.incrementAndGet();
      for (int b = 0; b < bucketCount; b++) {
         bucketHashes[segment].set(b, 0);
         bucketCounts[segment].set(b, 0);
      }
   }

   public void resetAllSegments() {
      for (int s = 0; s < bucketHashes.length; s++) {
         resetSegment(s);
      }
   }

   /**
    * Returns a snapshot of the bucket hashes for {@code segment}, or {@code null} if a
    * concurrent {@link #resetSegment} was detected mid-read (caller should fall back to a
    * full segment fetch).
    */
   public List<BucketHash> getBucketHashes(int segment) {
      int versionBefore = resetVersion.get();
      List<BucketHash> result = new ArrayList<>(bucketCount);
      for (int b = 0; b < bucketCount; b++) {
         result.add(new BucketHash(segment, b, bucketHashes[segment].get(b), bucketCounts[segment].get(b)));
      }
      if (resetVersion.get() != versionBefore) return null;
      return result;
   }

   public List<BucketHash> getAllBucketHashes(IntSet segments) {
      List<BucketHash> result = new ArrayList<>(segments.size() * bucketCount);
      segments.forEach((int seg) -> {
         List<BucketHash> segHashes = getBucketHashes(seg);
         if (segHashes != null) {
            result.addAll(segHashes);
         }
         // null means a concurrent reset raced this read; the segment is omitted from
         // the result map so findMismatchedBuckets returns null for it, triggering a
         // full segment fetch for that segment only.
      });
      return result;
   }

   void initForTest(Marshaller marshaller, int numSegments, int bucketCount) {
      this.marshaller = marshaller;
      this.bucketCount = bucketCount;
      this.enabled = true;
      this.hasStores = false;
      this.bucketHashes = new AtomicLongArray[numSegments];
      this.bucketCounts = new AtomicIntegerArray[numSegments];
      for (int i = 0; i < numSegments; i++) {
         bucketHashes[i] = new AtomicLongArray(bucketCount);
         bucketCounts[i] = new AtomicIntegerArray(bucketCount);
      }
   }
}
