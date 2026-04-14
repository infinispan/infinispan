package org.infinispan.server.resp.commands.countmin;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.resp.commands.MurmurHash64;
import org.infinispan.server.resp.commands.ProbabilisticErrors;

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
      long hash1 = MurmurHash64.hash(item, 0);
      long hash2 = MurmurHash64.hash(item, (int) hash1);

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
      long hash1 = MurmurHash64.hash(item, 0);
      long hash2 = MurmurHash64.hash(item, (int) hash1);

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
         throw new IllegalArgumentException(ProbabilisticErrors.CMS_WIDTH_DEPTH_MISMATCH);
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

}
