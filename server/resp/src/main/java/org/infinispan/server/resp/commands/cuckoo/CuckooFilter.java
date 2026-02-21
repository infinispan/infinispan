package org.infinispan.server.resp.commands.cuckoo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A Cuckoo filter implementation compatible with Redis CF commands.
 * <p>
 * Supports:
 * <ul>
 *   <li>Configurable capacity, bucket size, and max iterations</li>
 *   <li>Item deletion (unlike Bloom filters)</li>
 *   <li>Counting occurrences</li>
 *   <li>Scaling with multiple sub-filters</li>
 * </ul>
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_CUCKOO_FILTER)
public final class CuckooFilter {

   private final long capacity;
   private final int bucketSize;
   private final int maxIterations;
   private final int expansion;
   private final List<SubFilter> subFilters;
   private long itemsInserted;
   private long itemsDeleted;

   public CuckooFilter(long capacity, int bucketSize, int maxIterations, int expansion) {
      this.capacity = roundToPowerOfTwo(capacity);
      this.bucketSize = bucketSize;
      this.maxIterations = maxIterations;
      this.expansion = expansion;
      this.subFilters = new ArrayList<>();
      this.subFilters.add(new SubFilter(this.capacity, bucketSize));
      this.itemsInserted = 0;
      this.itemsDeleted = 0;
   }

   @ProtoFactory
   CuckooFilter(long capacity, int bucketSize, int maxIterations, int expansion,
                List<SubFilter> subFilters, long itemsInserted, long itemsDeleted) {
      this.capacity = capacity;
      this.bucketSize = bucketSize;
      this.maxIterations = maxIterations;
      this.expansion = expansion;
      this.subFilters = subFilters != null ? new ArrayList<>(subFilters) : new ArrayList<>();
      this.itemsInserted = itemsInserted;
      this.itemsDeleted = itemsDeleted;
   }

   @ProtoField(number = 1, defaultValue = "1024")
   public long getCapacity() {
      return capacity;
   }

   @ProtoField(number = 2, defaultValue = "2")
   public int getBucketSize() {
      return bucketSize;
   }

   @ProtoField(number = 3, defaultValue = "20")
   public int getMaxIterations() {
      return maxIterations;
   }

   @ProtoField(number = 4, defaultValue = "1")
   public int getExpansion() {
      return expansion;
   }

   @ProtoField(number = 5)
   public List<SubFilter> getSubFilters() {
      return subFilters;
   }

   @ProtoField(number = 6, defaultValue = "0")
   public long getItemsInserted() {
      return itemsInserted;
   }

   @ProtoField(number = 7, defaultValue = "0")
   public long getItemsDeleted() {
      return itemsDeleted;
   }

   /**
    * Adds an item to the Cuckoo filter. Allows duplicates.
    *
    * @param item the item to add
    * @return true if item was added, false if filter is full
    */
   public boolean add(byte[] item) {
      byte fingerprint = fingerprint(item);
      long hash = hash(item);

      for (SubFilter filter : subFilters) {
         if (filter.add(fingerprint, hash, maxIterations)) {
            itemsInserted++;
            return true;
         }
      }

      // Try to expand
      if (expansion > 0) {
         long newCapacity = roundToPowerOfTwo(subFilters.get(subFilters.size() - 1).getNumBuckets() * expansion);
         SubFilter newFilter = new SubFilter(newCapacity, bucketSize);
         if (newFilter.add(fingerprint, hash, maxIterations)) {
            subFilters.add(newFilter);
            itemsInserted++;
            return true;
         }
      }

      return false;
   }

   /**
    * Adds an item only if it doesn't already exist.
    *
    * @param item the item to add
    * @return true if item was newly added, false if it already exists or filter is full
    */
   public boolean addNx(byte[] item) {
      if (exists(item)) {
         return false;
      }
      return add(item);
   }

   /**
    * Checks if an item might exist in the filter.
    *
    * @param item the item to check
    * @return true if the item might exist, false if definitely not
    */
   public boolean exists(byte[] item) {
      byte fingerprint = fingerprint(item);
      long hash = hash(item);

      for (SubFilter filter : subFilters) {
         if (filter.contains(fingerprint, hash)) {
            return true;
         }
      }
      return false;
   }

   /**
    * Counts occurrences of an item in the filter.
    *
    * @param item the item to count
    * @return the count of occurrences
    */
   public long count(byte[] item) {
      byte fingerprint = fingerprint(item);
      long hash = hash(item);

      long count = 0;
      for (SubFilter filter : subFilters) {
         count += filter.count(fingerprint, hash);
      }
      return count;
   }

   /**
    * Deletes one occurrence of an item from the filter.
    *
    * @param item the item to delete
    * @return true if item was deleted, false if not found
    */
   public boolean delete(byte[] item) {
      byte fingerprint = fingerprint(item);
      long hash = hash(item);

      for (SubFilter filter : subFilters) {
         if (filter.delete(fingerprint, hash)) {
            itemsDeleted++;
            return true;
         }
      }
      return false;
   }

   /**
    * Returns the total number of buckets across all sub-filters.
    */
   public long getTotalBuckets() {
      return subFilters.stream().mapToLong(SubFilter::getNumBuckets).sum();
   }

   /**
    * Returns the total size in bytes.
    */
   public long getSize() {
      return subFilters.stream().mapToLong(SubFilter::getSizeInBytes).sum();
   }

   /**
    * Returns the number of sub-filters.
    */
   public int getFilterCount() {
      return subFilters.size();
   }

   private static long roundToPowerOfTwo(long n) {
      if (n <= 0) return 1;
      n--;
      n |= n >> 1;
      n |= n >> 2;
      n |= n >> 4;
      n |= n >> 8;
      n |= n >> 16;
      n |= n >> 32;
      return n + 1;
   }

   private static byte fingerprint(byte[] item) {
      long h = murmurHash64(item, 0x5bd1e995);
      byte fp = (byte) (h & 0xFF);
      return fp == 0 ? 1 : fp; // fingerprint must not be 0
   }

   private static long hash(byte[] item) {
      return murmurHash64(item, 0);
   }

   /*
    * This murmurhash64 implementation is different from that in org.infinispan.commons.hash.MurmurHash3 so it needs to
    * be local.
    */
   private static long murmurHash64(byte[] data, int seed) {
      final long c1 = 0x87c37b91114253d5L;
      final long c2 = 0x4cf5ad432745937fL;

      long h = seed;
      int len = data.length;
      int i = 0;

      while (i + 8 <= len) {
         long k = getLongLE(data, i);
         k *= c1;
         k = Long.rotateLeft(k, 31);
         k *= c2;
         h ^= k;
         h = Long.rotateLeft(h, 27);
         h = h * 5 + 0x52dce729;
         i += 8;
      }

      long k = 0;
      int remaining = len - i;
      if (remaining > 0) {
         for (int j = remaining - 1; j >= 0; j--) {
            k <<= 8;
            k |= (data[i + j] & 0xFFL);
         }
         k *= c1;
         k = Long.rotateLeft(k, 31);
         k *= c2;
         h ^= k;
      }

      h ^= len;
      h = fmix64(h);
      return h;
   }

   private static long getLongLE(byte[] data, int offset) {
      return (data[offset] & 0xFFL)
            | ((data[offset + 1] & 0xFFL) << 8)
            | ((data[offset + 2] & 0xFFL) << 16)
            | ((data[offset + 3] & 0xFFL) << 24)
            | ((data[offset + 4] & 0xFFL) << 32)
            | ((data[offset + 5] & 0xFFL) << 40)
            | ((data[offset + 6] & 0xFFL) << 48)
            | ((data[offset + 7] & 0xFFL) << 56);
   }

   private static long fmix64(long h) {
      h ^= h >>> 33;
      h *= 0xff51afd7ed558ccdL;
      h ^= h >>> 33;
      h *= 0xc4ceb9fe1a85ec53L;
      h ^= h >>> 33;
      return h;
   }

   /**
    * A sub-filter in a scalable Cuckoo filter.
    */
   @ProtoTypeId(ProtoStreamTypeIds.RESP_CUCKOO_SUB_FILTER)
   public static final class SubFilter {
      private final long numBuckets;
      private final int bucketSize;
      private final byte[] buckets;
      private final Random random = new Random();

      public SubFilter(long numBuckets, int bucketSize) {
         this.numBuckets = numBuckets;
         this.bucketSize = bucketSize;
         this.buckets = new byte[(int) (numBuckets * bucketSize)];
      }

      @ProtoFactory
      SubFilter(long numBuckets, int bucketSize, byte[] buckets) {
         this.numBuckets = numBuckets;
         this.bucketSize = bucketSize;
         this.buckets = buckets;
      }

      @ProtoField(number = 1, defaultValue = "1024")
      public long getNumBuckets() {
         return numBuckets;
      }

      @ProtoField(number = 2, defaultValue = "2")
      public int getBucketSize() {
         return bucketSize;
      }

      @ProtoField(number = 3)
      public byte[] getBuckets() {
         return buckets;
      }

      public long getSizeInBytes() {
         return buckets.length;
      }

      public boolean add(byte fingerprint, long hash, int maxIterations) {
         int i1 = (int) (Math.abs(hash) % numBuckets);
         int i2 = altIndex(i1, fingerprint);

         // Try to insert in bucket i1
         if (insertInBucket(i1, fingerprint)) {
            return true;
         }
         // Try to insert in bucket i2
         if (insertInBucket(i2, fingerprint)) {
            return true;
         }

         // Randomly pick one bucket and kick out entries
         int i = random.nextBoolean() ? i1 : i2;
         for (int n = 0; n < maxIterations; n++) {
            int slot = random.nextInt(bucketSize);
            int idx = (int) (i * bucketSize + slot);
            byte evicted = buckets[idx];
            buckets[idx] = fingerprint;
            fingerprint = evicted;

            i = altIndex(i, fingerprint);
            if (insertInBucket(i, fingerprint)) {
               return true;
            }
         }

         return false;
      }

      public boolean contains(byte fingerprint, long hash) {
         int i1 = (int) (Math.abs(hash) % numBuckets);
         int i2 = altIndex(i1, fingerprint);

         return containsInBucket(i1, fingerprint) || containsInBucket(i2, fingerprint);
      }

      public long count(byte fingerprint, long hash) {
         int i1 = (int) (Math.abs(hash) % numBuckets);
         int i2 = altIndex(i1, fingerprint);

         return countInBucket(i1, fingerprint) + countInBucket(i2, fingerprint);
      }

      public boolean delete(byte fingerprint, long hash) {
         int i1 = (int) (Math.abs(hash) % numBuckets);
         int i2 = altIndex(i1, fingerprint);

         return deleteFromBucket(i1, fingerprint) || deleteFromBucket(i2, fingerprint);
      }

      private int altIndex(int i, byte fingerprint) {
         long fpHash = CuckooFilter.murmurHash64(new byte[]{fingerprint}, 0x5bd1e995);
         return (int) (Math.abs(i ^ fpHash) % numBuckets);
      }

      private boolean insertInBucket(int bucketIdx, byte fingerprint) {
         int start = (int) (bucketIdx * bucketSize);
         for (int i = 0; i < bucketSize; i++) {
            if (buckets[start + i] == 0) {
               buckets[start + i] = fingerprint;
               return true;
            }
         }
         return false;
      }

      private boolean containsInBucket(int bucketIdx, byte fingerprint) {
         int start = (int) (bucketIdx * bucketSize);
         for (int i = 0; i < bucketSize; i++) {
            if (buckets[start + i] == fingerprint) {
               return true;
            }
         }
         return false;
      }

      private long countInBucket(int bucketIdx, byte fingerprint) {
         int start = (int) (bucketIdx * bucketSize);
         long count = 0;
         for (int i = 0; i < bucketSize; i++) {
            if (buckets[start + i] == fingerprint) {
               count++;
            }
         }
         return count;
      }

      private boolean deleteFromBucket(int bucketIdx, byte fingerprint) {
         int start = (int) (bucketIdx * bucketSize);
         for (int i = 0; i < bucketSize; i++) {
            if (buckets[start + i] == fingerprint) {
               buckets[start + i] = 0;
               return true;
            }
         }
         return false;
      }

   }

   public static final long DEFAULT_CAPACITY = 1024;
   public static final int DEFAULT_BUCKET_SIZE = 2;
   public static final int DEFAULT_MAX_ITERATIONS = 20;
   public static final int DEFAULT_EXPANSION = 1;
}
