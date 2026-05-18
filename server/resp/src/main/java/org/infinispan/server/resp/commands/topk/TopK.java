package org.infinispan.server.resp.commands.topk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.resp.commands.MurmurHash64;

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
   private final Map<WrappedByteArray, Long> topItems;

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
    * Increments the count of an item. Returns the expelled item if one was removed.
    *
    * @param item      the item to increment
    * @param increment the amount to add
    * @return the expelled item bytes, or null if no item was expelled
    */
   public byte[] incrBy(byte[] item, long increment) {
      WrappedByteArray wrappedItem = new WrappedByteArray(item);
      long fingerprint = MurmurHash64.hash(item, 0);

      long currentCount = getCount(item);
      long newCount = currentCount + increment;

      for (int i = 0; i < depth; i++) {
         int idx = (int) (Math.abs((fingerprint + i * fingerprint) % width));
         counters[i][idx] = Math.max(counters[i][idx], newCount);
      }

      if (topItems.containsKey(wrappedItem)) {
         topItems.put(wrappedItem, newCount);
         return null;
      }

      if (topItems.size() < k) {
         topItems.put(wrappedItem, newCount);
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

      if (minItem != null && newCount > minCount) {
         topItems.remove(minItem);
         topItems.put(wrappedItem, newCount);
         applyDecay(minItem.getBytes());
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

      long fingerprint = MurmurHash64.hash(item, 0);
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
      return topItems.containsKey(new WrappedByteArray(item));
   }

   /**
    * Returns the list of top-k items.
    *
    * @param withCount if true, includes counts
    * @return list of items (alternating item bytes, count if withCount)
    */
   public List<Object> list(boolean withCount) {
      List<Map.Entry<WrappedByteArray, Long>> sorted = new ArrayList<>(topItems.entrySet());
      sorted.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));

      List<Object> result = new ArrayList<>();
      for (Map.Entry<WrappedByteArray, Long> e : sorted) {
         result.add(new String(e.getKey().getBytes()));
         if (withCount) {
            result.add(e.getValue());
         }
      }
      return result;
   }

   private void applyDecay(byte[] item) {
      long fingerprint = MurmurHash64.hash(item, 0);
      for (int i = 0; i < depth; i++) {
         int idx = (int) (Math.abs((fingerprint + i * fingerprint) % width));
         counters[i][idx] = (long) (counters[i][idx] * decay);
      }
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
