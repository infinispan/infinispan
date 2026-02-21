package org.infinispan.server.resp.commands.topk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A Top-K implementation compatible with Redis TOPK commands.
 * <p>
 * Top-K is a probabilistic data structure that keeps track of the k most
 * frequent items in a data stream. It uses a Count-Min Sketch variant
 * with decay for counting and a min-heap for tracking top items.
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
   private final long[][] counters;
   private final Map<String, Long> topItems;

   /**
    * Creates a Top-K filter with specified parameters.
    *
    * @param k     number of top items to track
    * @param width number of counters per row
    * @param depth number of rows (hash functions)
    * @param decay decay constant for aging
    */
   public TopK(int k, int width, int depth, double decay) {
      this.k = k;
      this.width = width;
      this.depth = depth;
      this.decay = decay;
      this.counters = new long[depth][width];
      this.topItems = new HashMap<>();
   }

   @ProtoFactory
   TopK(int k, int width, int depth, double decay, long[] flatCounters, List<TopKEntry> entries) {
      this.k = k;
      this.width = width;
      this.depth = depth;
      this.decay = decay;
      this.counters = new long[depth][width];
      // Unflatten counters
      if (flatCounters != null) {
         for (int i = 0; i < depth && i * width < flatCounters.length; i++) {
            for (int j = 0; j < width && i * width + j < flatCounters.length; j++) {
               counters[i][j] = flatCounters[i * width + j];
            }
         }
      }
      this.topItems = new HashMap<>();
      if (entries != null) {
         for (TopKEntry entry : entries) {
            topItems.put(entry.getItem(), entry.getCount());
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
      for (Map.Entry<String, Long> e : topItems.entrySet()) {
         entries.add(new TopKEntry(e.getKey(), e.getValue()));
      }
      return entries;
   }

   /**
    * Adds an item to the Top-K. Returns the expelled item if one was removed.
    *
    * @param item the item to add
    * @return the expelled item name, or null if no item was expelled
    */
   public String add(byte[] item) {
      return incrBy(item, 1);
   }

   /**
    * Increments the count of an item. Returns the expelled item if one was removed.
    *
    * @param item      the item to increment
    * @param increment the amount to add
    * @return the expelled item name, or null if no item was expelled
    */
   public String incrBy(byte[] item, long increment) {
      String itemStr = new String(item);
      long fingerprint = hash(item);

      // Get current estimate
      long currentCount = getCount(item);
      long newCount = currentCount + increment;

      // Update counters
      for (int i = 0; i < depth; i++) {
         int idx = (int) (Math.abs((fingerprint + i * fingerprint) % width));
         counters[i][idx] = Math.max(counters[i][idx], newCount);
      }

      // Check if item is already in top-k
      if (topItems.containsKey(itemStr)) {
         topItems.put(itemStr, newCount);
         return null;
      }

      // If we have room, just add it
      if (topItems.size() < k) {
         topItems.put(itemStr, newCount);
         return null;
      }

      // Find the minimum item in top-k
      String minItem = null;
      long minCount = Long.MAX_VALUE;
      for (Map.Entry<String, Long> e : topItems.entrySet()) {
         if (e.getValue() < minCount) {
            minCount = e.getValue();
            minItem = e.getKey();
         }
      }

      // If new item has higher count than minimum, replace it
      if (minItem != null && newCount > minCount) {
         topItems.remove(minItem);
         topItems.put(itemStr, newCount);
         // Apply decay to the expelled item's fingerprint
         applyDecay(minItem.getBytes());
         return minItem;
      }

      return null;
   }

   /**
    * Returns the estimated count of an item.
    */
   public long getCount(byte[] item) {
      String itemStr = new String(item);
      if (topItems.containsKey(itemStr)) {
         return topItems.get(itemStr);
      }

      long fingerprint = hash(item);
      long minCount = Long.MAX_VALUE;
      for (int i = 0; i < depth; i++) {
         int idx = (int) (Math.abs((fingerprint + i * fingerprint) % width));
         minCount = Math.min(minCount, counters[i][idx]);
      }
      return minCount == Long.MAX_VALUE ? 0 : minCount;
   }

   /**
    * Checks if an item is in the Top-K.
    */
   public boolean query(byte[] item) {
      String itemStr = new String(item);
      return topItems.containsKey(itemStr);
   }

   /**
    * Returns the list of top-k items.
    *
    * @param withCount if true, includes counts
    * @return list of items (alternating item, count if withCount)
    */
   public List<Object> list(boolean withCount) {
      // Sort by count descending
      List<Map.Entry<String, Long>> sorted = new ArrayList<>(topItems.entrySet());
      sorted.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));

      List<Object> result = new ArrayList<>();
      for (Map.Entry<String, Long> e : sorted) {
         result.add(e.getKey());
         if (withCount) {
            result.add(e.getValue());
         }
      }
      return result;
   }

   private void applyDecay(byte[] item) {
      long fingerprint = hash(item);
      for (int i = 0; i < depth; i++) {
         int idx = (int) (Math.abs((fingerprint + i * fingerprint) % width));
         counters[i][idx] = (long) (counters[i][idx] * decay);
      }
   }

   private long hash(byte[] data) {
      // MurmurHash64
      final long c1 = 0x87c37b91114253d5L;
      final long c2 = 0x4cf5ad432745937fL;

      long h = 0;
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

   /**
    * Entry class for ProtoStream serialization of top items.
    */
   @ProtoTypeId(ProtoStreamTypeIds.RESP_TOP_K_ENTRY)
   public static final class TopKEntry {
      private final String item;
      private final long count;

      @ProtoFactory
      public TopKEntry(String item, long count) {
         this.item = item;
         this.count = count;
      }

      @ProtoField(number = 1)
      public String getItem() {
         return item;
      }

      @ProtoField(number = 2, defaultValue = "0")
      public long getCount() {
         return count;
      }
   }
}
