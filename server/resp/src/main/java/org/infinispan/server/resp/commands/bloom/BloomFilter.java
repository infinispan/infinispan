package org.infinispan.server.resp.commands.bloom;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A Bloom filter implementation compatible with Redis BF commands.
 * <p>
 * Supports:
 * <ul>
 *   <li>Configurable error rate and capacity</li>
 *   <li>Scaling with multiple sub-filters</li>
 *   <li>Non-scaling mode</li>
 * </ul>
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_BLOOM_FILTER)
public final class BloomFilter {

   private final double errorRate;
   private final long capacity;
   private final int expansion;
   private final boolean nonScaling;
   private final List<SubFilter> subFilters;
   private long itemCount;

   public BloomFilter(double errorRate, long capacity, int expansion, boolean nonScaling) {
      this.errorRate = errorRate;
      this.capacity = capacity;
      this.expansion = expansion;
      this.nonScaling = nonScaling;
      this.subFilters = new ArrayList<>();
      this.itemCount = 0;
      this.subFilters.add(new SubFilter(capacity, errorRate));
   }

   @ProtoFactory
   BloomFilter(double errorRate, long capacity, int expansion, boolean nonScaling,
               List<SubFilter> subFilters, long itemCount) {
      this.errorRate = errorRate;
      this.capacity = capacity;
      this.expansion = expansion;
      this.nonScaling = nonScaling;
      this.subFilters = subFilters != null ? new ArrayList<>(subFilters) : new ArrayList<>();
      this.itemCount = itemCount;
   }

   @ProtoField(number = 1, defaultValue = "0.01")
   public double getErrorRate() {
      return errorRate;
   }

   @ProtoField(number = 2, defaultValue = "100")
   public long getCapacity() {
      return capacity;
   }

   @ProtoField(number = 3, defaultValue = "2")
   public int getExpansion() {
      return expansion;
   }

   @ProtoField(number = 4, defaultValue = "false")
   public boolean isNonScaling() {
      return nonScaling;
   }

   @ProtoField(number = 5)
   public List<SubFilter> getSubFilters() {
      return subFilters;
   }

   @ProtoField(number = 6, defaultValue = "0")
   public long getItemCount() {
      return itemCount;
   }

   /**
    * Adds an item to the Bloom filter.
    *
    * @param item the item to add
    * @return true if the item was newly added, false if it probably already existed
    * @throws IllegalStateException if the filter is full and non-scaling
    */
   public boolean add(byte[] item) {
      if (mightContain(item)) {
         return false;
      }

      SubFilter currentFilter = subFilters.get(subFilters.size() - 1);
      if (currentFilter.isFull()) {
         if (nonScaling) {
            throw new IllegalStateException("ERR non scaling filter is full");
         }
         long newCapacity = (long) (currentFilter.getCapacity() * expansion);
         double newErrorRate = errorRate * Math.pow(0.5, subFilters.size());
         currentFilter = new SubFilter(newCapacity, newErrorRate);
         subFilters.add(currentFilter);
      }

      currentFilter.add(item);
      itemCount++;
      return true;
   }

   /**
    * Checks if an item might be contained in the Bloom filter.
    *
    * @param item the item to check
    * @return true if the item might be in the filter (with false positive probability),
    * false if the item is definitely not in the filter
    */
   public boolean mightContain(byte[] item) {
      for (SubFilter filter : subFilters) {
         if (filter.mightContain(item)) {
            return true;
         }
      }
      return false;
   }

   /**
    * Returns the total capacity across all sub-filters.
    */
   public long getTotalCapacity() {
      return subFilters.stream().mapToLong(SubFilter::getCapacity).sum();
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

   /**
    * A sub-filter in a scalable Bloom filter.
    */
   @ProtoTypeId(ProtoStreamTypeIds.RESP_BLOOM_SUB_FILTER)
   public static final class SubFilter {
      private final long capacity;
      private final int numHashFunctions;
      private final byte[] bits;
      private long count;

      public SubFilter(long capacity, double errorRate) {
         this.capacity = capacity;
         int numBits = optimalNumOfBits(capacity, errorRate);
         this.numHashFunctions = optimalNumOfHashFunctions(capacity, numBits);
         this.bits = new byte[(numBits + 7) / 8];
         this.count = 0;
      }

      @ProtoFactory
      SubFilter(long capacity, int numHashFunctions, byte[] bits, long count) {
         this.capacity = capacity;
         this.numHashFunctions = numHashFunctions;
         this.bits = bits;
         this.count = count;
      }

      @ProtoField(number = 1, defaultValue = "100")
      public long getCapacity() {
         return capacity;
      }

      @ProtoField(number = 2, defaultValue = "7")
      public int getNumHashFunctions() {
         return numHashFunctions;
      }

      @ProtoField(number = 3)
      public byte[] getBits() {
         return bits;
      }

      @ProtoField(number = 4, defaultValue = "0")
      public long getCount() {
         return count;
      }

      public boolean isFull() {
         return count >= capacity;
      }

      public long getSizeInBytes() {
         return bits.length;
      }

      public void add(byte[] item) {
         long[] hashes = hash(item);
         for (int i = 0; i < numHashFunctions; i++) {
            int bitIndex = getBitIndex(hashes, i);
            setBit(bitIndex);
         }
         count++;
      }

      public boolean mightContain(byte[] item) {
         long[] hashes = hash(item);
         for (int i = 0; i < numHashFunctions; i++) {
            int bitIndex = getBitIndex(hashes, i);
            if (!getBit(bitIndex)) {
               return false;
            }
         }
         return true;
      }

      private int getBitIndex(long[] hashes, int i) {
         long combinedHash = hashes[0] + (long) i * hashes[1];
         return (int) (Math.abs(combinedHash) % (bits.length * 8L));
      }

      private void setBit(int index) {
         bits[index / 8] |= (byte) (1 << (index % 8));
      }

      private boolean getBit(int index) {
         return (bits[index / 8] & (1 << (index % 8))) != 0;
      }

      private long[] hash(byte[] data) {
         return MurmurHash3.MurmurHash3_x64_128(data, 0);
      }

      /**
       * Calculates the optimal number of bits for the given capacity and error rate.
       */
      private static int optimalNumOfBits(long n, double p) {
         if (p == 0) {
            p = Double.MIN_VALUE;
         }
         return (int) Math.ceil(-n * Math.log(p) / (Math.log(2) * Math.log(2)));
      }

      /**
       * Calculates the optimal number of hash functions for the given capacity and bit size.
       */
      private static int optimalNumOfHashFunctions(long n, int m) {
         return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
      }
   }

   /**
    * Default values used when creating a filter with BF.ADD without BF.RESERVE.
    */
   public static final double DEFAULT_ERROR_RATE = 0.01;
   public static final long DEFAULT_CAPACITY = 100;
   public static final int DEFAULT_EXPANSION = 2;
}
