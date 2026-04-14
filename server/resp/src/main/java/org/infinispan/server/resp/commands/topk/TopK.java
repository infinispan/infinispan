package org.infinispan.server.resp.commands.topk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.resp.commands.MurmurHash64;

/**
 * A Top-K implementation based on the HeavyKeeper algorithm.
 * <p>
 * HeavyKeeper is a probabilistic data structure that tracks the k most
 * frequent items in a data stream. Each cell in the count array stores
 * a fingerprint and a counter. During insertion, cells with matching
 * fingerprints are incremented, while non-matching cells are probabilistically
 * decayed with probability {@code decay^counter}, allowing frequent items
 * (elephant flows) to persist while infrequent items (mouse flows) age out.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_TOP_K)
public final class TopK {

   public static final int DEFAULT_WIDTH = 8;
   public static final int DEFAULT_DEPTH = 7;
   public static final double DEFAULT_DECAY = 0.9;

   private final int k;
   private final int width;
   private final int depth;
   private final double decay;
   private final long[][] fingerprints;
   private final long[][] counters;
   private final Map<WrappedByteArray, Long> topItems;

   /**
    * Creates a Top-K filter with specified parameters.
    *
    * @param k     number of top items to track
    * @param width number of buckets per row
    * @param depth number of rows (hash functions)
    * @param decay decay constant for probabilistic aging
    */
   public TopK(int k, int width, int depth, double decay) {
      this.k = k;
      this.width = width;
      this.depth = depth;
      this.decay = decay;
      this.fingerprints = new long[depth][width];
      this.counters = new long[depth][width];
      this.topItems = new HashMap<>();
   }

   @ProtoFactory
   TopK(int k, int width, int depth, double decay, long[] flatCounters,
         List<TopKEntry> entries, long[] flatFingerprints) {
      this.k = k;
      this.width = width;
      this.depth = depth;
      this.decay = decay;
      this.fingerprints = new long[depth][width];
      this.counters = new long[depth][width];
      if (flatCounters != null) {
         for (int i = 0; i < depth && i * width < flatCounters.length; i++) {
            for (int j = 0; j < width && i * width + j < flatCounters.length; j++) {
               counters[i][j] = flatCounters[i * width + j];
            }
         }
      }
      if (flatFingerprints != null) {
         for (int i = 0; i < depth && i * width < flatFingerprints.length; i++) {
            for (int j = 0; j < width && i * width + j < flatFingerprints.length; j++) {
               fingerprints[i][j] = flatFingerprints[i * width + j];
            }
         }
      }
      this.topItems = new HashMap<>();
      if (entries != null) {
         for (TopKEntry entry : entries) {
            topItems.put(new WrappedByteArray(entry.getItem()), entry.getCount());
         }
      }
   }

   @ProtoField(number = 1, defaultValue = "10")
   public int getK() {
      return k;
   }

   @ProtoField(number = 2, defaultValue = "8")
   public int getWidth() {
      return width;
   }

   @ProtoField(number = 3, defaultValue = "7")
   public int getDepth() {
      return depth;
   }

   @ProtoField(number = 4, defaultValue = "0.9")
   public double getDecay() {
      return decay;
   }

   @ProtoField(number = 5)
   public long[] getFlatCounters() {
      long[] flat = new long[depth * width];
      for (int i = 0; i < depth; i++) {
         for (int j = 0; j < width; j++) {
            flat[i * width + j] = counters[i][j];
         }
      }
      return flat;
   }

   @ProtoField(number = 6)
   public List<TopKEntry> getEntries() {
      List<TopKEntry> entries = new ArrayList<>();
      for (Map.Entry<WrappedByteArray, Long> e : topItems.entrySet()) {
         entries.add(new TopKEntry(e.getKey().getBytes(), e.getValue()));
      }
      return entries;
   }

   @ProtoField(number = 7)
   public long[] getFlatFingerprints() {
      long[] flat = new long[depth * width];
      for (int i = 0; i < depth; i++) {
         for (int j = 0; j < width; j++) {
            flat[i * width + j] = fingerprints[i][j];
         }
      }
      return flat;
   }

   /**
    * Adds an item to the Top-K. Returns the expelled item if one was removed.
    *
    * @param item the item to add
    * @return the expelled item bytes, or null if no item was expelled
    */
   public byte[] add(byte[] item) {
      return incrBy(item, 1);
   }

   /**
    * Increments the count of an item using HeavyKeeper insertion.
    * <p>
    * For each CMS cell at the item's hash position:
    * <ul>
    *   <li>If empty (counter=0): claim the cell with the item's fingerprint</li>
    *   <li>If fingerprint matches: increment the counter</li>
    *   <li>If fingerprint differs: with probability decay^counter, decrement;
    *       if counter reaches 0, replace with the new item's fingerprint</li>
    * </ul>
    *
    * @param item      the item to increment
    * @param increment the amount to add
    * @return the expelled item bytes, or null if no item was expelled
    */
   public byte[] incrBy(byte[] item, long increment) {
      WrappedByteArray wrappedItem = new WrappedByteArray(item);
      long fp = MurmurHash64.hash(item, 0);
      long hash2 = MurmurHash64.hash(item, (int) fp);

      // Phase 1: HeavyKeeper CMS update
      for (int i = 0; i < depth; i++) {
         int idx = getIndex(fp, hash2, i);
         if (counters[i][idx] == 0) {
            fingerprints[i][idx] = fp;
            counters[i][idx] = increment;
         } else if (fingerprints[i][idx] == fp) {
            counters[i][idx] += increment;
         } else {
            double prob = Math.pow(decay, counters[i][idx]);
            if (ThreadLocalRandom.current().nextDouble() < prob) {
               counters[i][idx]--;
               if (counters[i][idx] == 0) {
                  fingerprints[i][idx] = fp;
                  counters[i][idx] = increment;
               }
            }
         }
      }

      // Phase 2: update top-k
      if (topItems.containsKey(wrappedItem)) {
         topItems.merge(wrappedItem, increment, Long::sum);
         return null;
      }

      long estimatedCount = getMaxMatchingCount(fp, hash2);
      if (estimatedCount == 0) {
         return null;
      }

      if (topItems.size() < k) {
         topItems.put(wrappedItem, estimatedCount);
         return null;
      }

      WrappedByteArray minItem = null;
      long minCount = Long.MAX_VALUE;
      for (Map.Entry<WrappedByteArray, Long> e : topItems.entrySet()) {
         if (e.getValue() < minCount) {
            minCount = e.getValue();
            minItem = e.getKey();
         }
      }

      if (minItem != null && estimatedCount > minCount) {
         topItems.remove(minItem);
         topItems.put(wrappedItem, estimatedCount);
         return minItem.getBytes();
      }

      return null;
   }

   /**
    * Returns the estimated count of an item.
    */
   public long getCount(byte[] item) {
      WrappedByteArray wrappedItem = new WrappedByteArray(item);
      Long count = topItems.get(wrappedItem);
      if (count != null) {
         return count;
      }

      long fp = MurmurHash64.hash(item, 0);
      long hash2 = MurmurHash64.hash(item, (int) fp);
      return getMaxMatchingCount(fp, hash2);
   }

   /**
    * Checks if an item is in the Top-K.
    */
   public boolean query(byte[] item) {
      return topItems.containsKey(new WrappedByteArray(item));
   }

   /**
    * Returns the list of top-k items sorted by count descending.
    *
    * @param withCount if true, includes counts interleaved with items
    * @return list of items (byte[]) and optionally counts (Long)
    */
   public List<Object> list(boolean withCount) {
      List<Map.Entry<WrappedByteArray, Long>> sorted = new ArrayList<>(topItems.entrySet());
      sorted.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));

      List<Object> result = new ArrayList<>();
      for (Map.Entry<WrappedByteArray, Long> e : sorted) {
         result.add(e.getKey().getBytes());
         if (withCount) {
            result.add(e.getValue());
         }
      }
      return result;
   }

   private long getMaxMatchingCount(long fp, long hash2) {
      long maxCount = 0;
      for (int i = 0; i < depth; i++) {
         int idx = getIndex(fp, hash2, i);
         if (fingerprints[i][idx] == fp) {
            maxCount = Math.max(maxCount, counters[i][idx]);
         }
      }
      return maxCount;
   }

   private int getIndex(long hash1, long hash2, int row) {
      long combinedHash = hash1 + (long) row * hash2;
      return (int) (Math.abs(combinedHash) % width);
   }

   /**
    * Entry class for ProtoStream serialization of top items.
    */
   @ProtoTypeId(ProtoStreamTypeIds.RESP_TOP_K_ENTRY)
   public static final class TopKEntry {
      private final byte[] item;
      private final long count;

      @ProtoFactory
      public TopKEntry(byte[] item, long count) {
         this.item = item;
         this.count = count;
      }

      @ProtoField(number = 1)
      public byte[] getItem() {
         return item;
      }

      @ProtoField(number = 2, defaultValue = "0")
      public long getCount() {
         return count;
      }
   }
}
