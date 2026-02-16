package org.infinispan.server.resp.commands.countmin;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A Count-Min Sketch implementation compatible with Redis CMS commands.
 * <p>
 * Count-Min Sketch is a probabilistic data structure that serves as a frequency table
 * of events in a stream of data. It uses hash functions to map events to frequencies,
 * but unlike a hash table, uses only sub-linear space at the expense of overcounting
 * some events due to collisions.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_COUNT_MIN_SKETCH)
public final class CountMinSketch {

   private final int width;
   private final int depth;
   private final long[] counters;
   private long totalCount;

   /**
    * Creates a Count-Min Sketch with specified dimensions.
    *
    * @param width number of counters in each row (determines error rate)
    * @param depth number of rows/hash functions (determines probability)
    */
   public CountMinSketch(int width, int depth) {
      this.width = width;
      this.depth = depth;
      this.counters = new long[width * depth];
      this.totalCount = 0;
   }

   /**
    * Creates a Count-Min Sketch based on error and probability tolerances.
    *
    * @param error       desired error rate (e.g., 0.001 for 0.1%)
    * @param probability desired probability of error (e.g., 0.01 for 1%)
    * @return a new CountMinSketch with appropriate dimensions
    */
   public static CountMinSketch fromProbability(double error, double probability) {
      int width = (int) Math.ceil(2.0 / error);
      int depth = (int) Math.ceil(Math.log(1.0 / probability));
      return new CountMinSketch(width, depth);
   }

   @ProtoFactory
   CountMinSketch(int width, int depth, long[] counters, long totalCount) {
      this.width = width;
      this.depth = depth;
      this.counters = counters;
      this.totalCount = totalCount;
   }

   @ProtoField(number = 1, defaultValue = "2000")
   public int getWidth() {
      return width;
   }

   @ProtoField(number = 2, defaultValue = "7")
   public int getDepth() {
      return depth;
   }

   @ProtoField(number = 3)
   public long[] getCounters() {
      return counters;
   }

   @ProtoField(number = 4, defaultValue = "0")
   public long getTotalCount() {
      return totalCount;
   }

   /**
    * Increments the count of an item by the specified amount.
    *
    * @param item      the item to increment
    * @param increment the amount to add
    * @return the estimated count after increment
    */
   public long incrBy(byte[] item, long increment) {
      long minCount = Long.MAX_VALUE;
      long hash1 = murmurHash64(item, 0);
      long hash2 = murmurHash64(item, (int) hash1);

      for (int i = 0; i < depth; i++) {
         int index = getIndex(hash1, hash2, i);
         counters[index] += increment;
         minCount = Math.min(minCount, counters[index]);
      }

      totalCount += increment;
      return minCount;
   }

   /**
    * Returns the estimated count of an item.
    *
    * @param item the item to query
    * @return the estimated count (minimum across all hash functions)
    */
   public long query(byte[] item) {
      long minCount = Long.MAX_VALUE;
      long hash1 = murmurHash64(item, 0);
      long hash2 = murmurHash64(item, (int) hash1);

      for (int i = 0; i < depth; i++) {
         int index = getIndex(hash1, hash2, i);
         minCount = Math.min(minCount, counters[index]);
      }

      return minCount;
   }

   /**
    * Merges another sketch into this one with a weight multiplier.
    *
    * @param other  the sketch to merge
    * @param weight the weight multiplier for the other sketch
    * @throws IllegalArgumentException if dimensions don't match
    */
   public void merge(CountMinSketch other, double weight) {
      if (this.width != other.width || this.depth != other.depth) {
         throw new IllegalArgumentException("ERR width/depth doesn't match");
      }

      for (int i = 0; i < counters.length; i++) {
         counters[i] += (long) (other.counters[i] * weight);
      }
      totalCount += (long) (other.totalCount * weight);
   }

   private int getIndex(long hash1, long hash2, int row) {
      long combinedHash = hash1 + (long) row * hash2;
      int col = (int) (Math.abs(combinedHash) % width);
      return row * width + col;
   }

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
      h ^= h >>> 33;
      h *= 0xff51afd7ed558ccdL;
      h ^= h >>> 33;
      h *= 0xc4ceb9fe1a85ec53L;
      h ^= h >>> 33;
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

   public static final int DEFAULT_WIDTH = 2000;
   public static final int DEFAULT_DEPTH = 7;
}
