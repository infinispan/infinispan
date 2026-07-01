package org.infinispan.server.resp.commands.topk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToLongBiFunction;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.stat.HeavyKeeper;
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

   private static final ToLongBiFunction<WrappedByteArray, Integer> HASH_FUNCTION =
         (wba, seed) -> MurmurHash64.hash(wba.getBytes(), seed);

   public static final int DEFAULT_WIDTH = 8;
   public static final int DEFAULT_DEPTH = 7;
   public static final double DEFAULT_DECAY = 0.9;

   private final Holder holder;

   /**
    * Creates a Top-K filter with specified parameters.
    *
    * @param k     number of top items to track
    * @param width number of buckets per row
    * @param depth number of rows (hash functions)
    * @param decay decay constant for probabilistic aging
    */
   public TopK(int k, int width, int depth, double decay) {
      this.holder = new Holder(k, width, depth, decay, HASH_FUNCTION);
   }

   @ProtoFactory
   TopK(int k, int width, int depth, double decay, long[] flatCounters,
         List<TopKEntry> entries, long[] flatFingerprints) {
      long[][] fingerprints = new long[depth][width];
      long[][] counters = new long[depth][width];
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
      Map<WrappedByteArray, Long> topItems = new HashMap<>();
      if (entries != null) {
         for (TopKEntry entry : entries) {
            topItems.put(new WrappedByteArray(entry.getItem()), entry.getCount());
         }
      }

      this.holder = new Holder(k, width, depth, decay, HASH_FUNCTION, fingerprints, counters, topItems);
   }

   @ProtoField(number = 1, defaultValue = "10")
   public int getK() {
      return holder.getK();
   }

   @ProtoField(number = 2, defaultValue = "8")
   public int getWidth() {
      return holder.getWidth();
   }

   @ProtoField(number = 3, defaultValue = "7")
   public int getDepth() {
      return holder.getDepth();
   }

   @ProtoField(number = 4, defaultValue = "0.9")
   public double getDecay() {
      return holder.getDecay();
   }

   @ProtoField(number = 5)
   public long[] getFlatCounters() {
      return holder.getFlatCounters();
   }

   @ProtoField(number = 6)
   public List<TopKEntry> getEntries() {
      return holder.getEntries();
   }

   @ProtoField(number = 7)
   public long[] getFlatFingerprints() {
      return holder.getFlatFingerprints();
   }

   /**
    * Adds an item to the Top-K. Returns the expelled item if one was removed.
    *
    * @param item the item to add
    * @return the expelled item bytes, or null if no item was expelled
    */
   public byte[] add(byte[] item) {
      WrappedByteArray wba = holder.add(new WrappedByteArray(item));
      return wba != null ? wba.getBytes() : null;
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
      WrappedByteArray res = holder.incrBy(wrappedItem, increment);
      return res != null ? res.getBytes() : null;
   }

   /**
    * Returns the estimated count of an item.
    */
   public long getCount(byte[] item) {
      WrappedByteArray wrappedItem = new WrappedByteArray(item);
      return holder.getCount(wrappedItem);
   }

   /**
    * Checks if an item is in the Top-K.
    */
   public boolean query(byte[] item) {
      return holder.query(new WrappedByteArray(item));
   }

   /**
    * Returns the list of top-k items sorted by count descending.
    *
    * @param withCount if true, includes counts interleaved with items
    * @return list of items (byte[]) and optionally counts (Long)
    */
   public List<Object> list(boolean withCount) {
      List<Object> result = new ArrayList<>();
      for (HeavyKeeper.KeyFrequency<WrappedByteArray> item : holder.list()) {
         result.add(item.key().getBytes());
         if (withCount) {
            result.add(item.count());
         }
      }
      return result;
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

   private static final class Holder extends HeavyKeeper<WrappedByteArray> {

      public Holder(int k, int width, int depth, double decay, ToLongBiFunction<WrappedByteArray, Integer> hash) {
         super(k, width, depth, decay, hash);
      }

      Holder(int k, int width, int depth, double decay, ToLongBiFunction<WrappedByteArray, Integer> hash,
             long[][] fingerprints, long[][] counters, Map<WrappedByteArray, Long> topItems) {
         super(k, width, depth, decay, hash, fingerprints, counters, topItems);
      }

      public long[] getFlatCounters() {
         int depth = getDepth();
         int width = getWidth();
         long[][] counters = counters();
         long[] flat = new long[depth * width];
         for (int i = 0; i < depth; i++) {
            System.arraycopy(counters[i], 0, flat, i * width, width);
         }
         return flat;
      }

      public long[] getFlatFingerprints() {
         int depth = getDepth();
         int width = getWidth();
         long[][] fingerprints = fingerprints();
         long[] flat = new long[depth * width];
         for (int i = 0; i < depth; i++) {
            System.arraycopy(fingerprints[i], 0, flat, i * width, width);
         }
         return flat;
      }

      public List<TopKEntry> getEntries() {
         List<TopKEntry> entries = new ArrayList<>();
         for (Map.Entry<WrappedByteArray, Long> e : topItems().entrySet()) {
            WrappedByteArray item = e.getKey();
            entries.add(new TopKEntry(item.getBytes(), e.getValue()));
         }
         return entries;
      }
   }
}
