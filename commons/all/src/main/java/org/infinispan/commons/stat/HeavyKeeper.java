package org.infinispan.commons.stat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.ToLongBiFunction;

/**
 * A Top-K implementation based on the HeavyKeeper algorithm.
 *
 * <p>
 * HeavyKeeper is a probabilistic data structure that tracks the k most
 * frequent items in a data stream. Each cell in the count array stores
 * a fingerprint and a counter. During insertion, cells with matching
 * fingerprints are incremented, while non-matching cells are probabilistically
 * decayed with probability {@code decay^counter}, allowing frequent items
 * (elephant flows) to persist while infrequent items (mouse flows) age out.
 * </p>
 *
 * <p>
 * This class is not designed for arbitrary extension. Subclassing is permitted solely to support serialization in modules
 * that cannot access internal state through the public API.
 * </p>
 *
 * @since 16.3
 */
public class HeavyKeeper<T> {

   // This is how many elements the data structure will track.
   private final int k;
   private final int width;
   private final int depth;
   private final double decay;
   private final ToLongBiFunction<T, Integer> hash;

   // The sketching mechanism.
   // In one array we store an object fingerprints and in the other the frequency.
   // This combination creates the probabilistic nature that doesn't provide an exact count, but close enough.
   private final long[][] fingerprints;
   private final long[][] counters;

   // The exact count of the current top-k elements.
   // After an object in the sketch reaches a minimum, it moves to an exact count in the map.
   private final Map<T, Long> topItems;

   /**
    * Creates a new HeavyKeeper tracker.
    *
    * @param k number of top items to track
    * @param width number of buckets per row in the sketch
    * @param depth number of rows in the sketch
    * @param decay decay constant for probabilistic aging, between 0 exclusive and 1 exclusive
    * @param hash hash function mapping (key, seed) to a long hash value
    */
   public HeavyKeeper(int k, int width, int depth, double decay, ToLongBiFunction<T, Integer> hash) {
      if (decay <= 0 || decay >= 1)
         throw new IllegalArgumentException(String.format("Invalid decay value for heavy keeper. Should follow 0 < %f < 1", decay));

      this.k = k;
      this.width = width;
      this.depth = depth;
      this.decay = decay;
      this.hash = hash;
      this.fingerprints = new long[depth][width];
      this.counters = new long[depth][width];
      this.topItems = new HashMap<>();
   }

   /**
    * Reconstructs a HeavyKeeper from previously serialized state.
    *
    * @param k number of top items to track
    * @param width number of buckets per row in the sketch
    * @param depth number of rows in the sketch
    * @param decay decay constant for probabilistic aging
    * @param hash hash function mapping (key, seed) to a long hash value
    * @param fingerprints the sketch fingerprint array, indexed by [depth][width]
    * @param counters the sketch counter array, indexed by [depth][width]
    * @param topItems the top-k entries mapping items to their counts
    */
   protected HeavyKeeper(int k, int width, int depth, double decay, ToLongBiFunction<T, Integer> hash,
                         long[][] fingerprints, long[][] counters, Map<T, Long> topItems) {
      this.k = k;
      this.width = width;
      this.depth = depth;
      this.decay = decay;
      this.hash = hash;
      this.fingerprints = fingerprints;
      this.counters = counters;
      this.topItems = topItems;
   }

   /**
    * Adds an item with a count of one.
    *
    * @param key the item to add
    * @return the displaced item if one was expelled from the top-k, or {@code null}
    */
   public final T add(T key) {
      return incrBy(key, 1);
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
    * @param key       the item to increment
    * @param increment the amount to add
    * @return the expelled item, or null if no item was expelled
    */
   public final T incrBy(T key, long increment) {
      // Two hashes per each element.
      // This hash contains the fingerprint to identify the cells in the sketch.
      long fp = hash.applyAsLong(key, 0);

      // This hash combines with the fingerprint to double-hash across the rows.
      long hash2 = hash.applyAsLong(key, (int) fp);

      // Every key updates the sketch, regardless of getting into the top-k elements.
      for (int i = 0; i < depth; i++) {
         int idx = index(fp, hash2, i);

         // This cell is empty.
         // This object will claim it with its fingerprints.
         if (counters[i][idx] == 0) {
            fingerprints[i][idx] = fp;
            counters[i][idx] = increment;
            continue;
         }

         // The fingerprint in the cell matches this object fingerprint.
         // This way, we only update the counting.
         if (fingerprints[i][idx] == fp) {
            counters[i][idx] += increment;
            continue;
         }

         // There is a collision. The cell is not empty and the fingerprints don't match.
         // We utilize some probability to decay this counter.
         // Counter with a high count will be less likely to decay.
         // This means that frequent elements resist displacements, while low hitters are more likely to fade.
         double prob = Math.pow(decay, counters[i][idx]);
         if (ThreadLocalRandom.current().nextDouble() < prob) {
            // The counter reaches zero, so we have a vacant cell for the current object.
            if (--counters[i][idx] == 0) {
               fingerprints[i][idx] = fp;
               counters[i][idx] = increment;
            }
         }
      }

      // After updating the sketches, the top-k map needs to be updated accordingly.

      // This object is already tracked, so we simply increment the counter.
      if (topItems.containsKey(key)) {
         topItems.merge(key, increment, Long::sum);
         return null;
      }

      // This object is not tracked in the top-k.
      // We verify whether it got enough hits in the sketch to be promoted to the top-k map.
      long estimatedCount = maxMatchingCount(fp, hash2);
      if (estimatedCount == 0)
         return null;

      // The item is promoted and the maps has enough room for a new object.
      if (topItems.size() < k) {
         topItems.put(key, estimatedCount);
         return null;
      }

      // The item was promoted but the map is full.
      // We identiy which is the current object with the smallest count in top-k.
      // Observe this affects only the top-k map. The sketch still tracks the object.
      T minItem = null;
      long minCount = Long.MAX_VALUE;
      for (Map.Entry<T, Long> entry : topItems.entrySet()) {
         if (entry.getValue() < minCount) {
            minCount = entry.getValue();
            minItem = entry.getKey();
         }
      }

      // We only replace if the current object has an estimated count that beats the minimum.
      if (minItem != null && estimatedCount > minCount) {
         topItems.remove(minItem);
         topItems.put(key, estimatedCount);
         return minItem;
      }

      return null;
   }

   /**
    * Returns the estimated count of an item. For items currently in the top-k,
    * the count is exact from the moment of promotion onward.
    *
    * @param key the item to query
    * @return the estimated count, or zero if the item has not been observed
    */
   public final long getCount(T key) {
      Long count = topItems.get(key);
      if (count != null)
         return count;

      // Same approach when counting to add objects.
      long fp = hash.applyAsLong(key, 0);
      long hash2 = hash.applyAsLong(key, (int) fp);
      return maxMatchingCount(fp, hash2);
   }

   /**
    * Checks whether an item is currently in the top-k.
    *
    * @param key the item to check
    * @return {@code true} if the item is in the top-k
    */
   public final boolean query(T key) {
      return topItems.containsKey(key);
   }

   /**
    * Returns the current top-k items sorted by count in descending order.
    *
    * @return a list of key-frequency pairs, highest count first
    */
   public final List<KeyFrequency<T>> list() {
      List<KeyFrequency<T>> result = new ArrayList<>(topItems.size());
      for (Map.Entry<T, Long> entry : topItems.entrySet()) {
         result.add(new KeyFrequency<>(entry.getKey(), entry.getValue()));
      }
      result.sort(KeyFrequency.COMPARATOR);
      return result;
   }

   /**
    * Resets the tracker to its initial empty state, clearing both the sketch and the top-k map.
    */
   public final void reset() {
      for (long[] row : fingerprints) {
         Arrays.fill(row, 0);
      }

      for (long[] row : counters) {
         Arrays.fill(row, 0);
      }

      topItems.clear();
   }

   /**
    * @return the number of top items tracked
    */
   public final int getK() {
      return k;
   }

   /**
    * @return the number of buckets per row in the sketch
    */
   public final int getWidth() {
      return width;
   }

   /**
    * @return the number of rows in the sketch
    */
   public final int getDepth() {
      return depth;
   }

   /**
    * @return the decay constant for probabilistic aging
    */
   public final double getDecay() {
      return decay;
   }

   /**
    * A key and its associated frequency count.
    *
    * @param key   the tracked item
    * @param count the frequency count
    * @since 16.2
    */
   public record KeyFrequency<T>(T key, long count) {
      private static final Comparator<KeyFrequency<?>> COMPARATOR =
            (a, b) -> Long.compare(b.count, a.count);
   }

   /**
    * Returns the sketch fingerprint array. Intended for serialization in subclasses.
    *
    * @return the fingerprint array indexed by [depth][width]
    */
   protected final long[][] fingerprints() {
      return fingerprints;
   }

   /**
    * Returns the counter array. Intended for serialization in subclasses.
    *
    * @return the counter array indexed by [depth][width]
    */
   protected final long[][] counters() {
      return counters;
   }

   /**
    * Returns the top-k map of items to their counts. Intended for serialization in subclasses.
    *
    * @return the top-k map
    */
   protected final Map<T, Long> topItems() {
      return Collections.unmodifiableMap(topItems);
   }

   /**
    * Queries the sketch for the maximum counter across all rows where the fingerprint matches.
    *
    * <p>
    * This is the conservative estimate. Taking the max of matching cells rather than the min of all cells avoids over-counting
    * from fingerprint collisions in non-matching rows.
    * </p>
    *
    * @return the maximum count in the matching cells.
    */
   private long maxMatchingCount(long fp, long hash2) {
      long maxCount = 0;
      for (int i = 0; i < depth; i++) {
         int idx = index(fp, hash2, i);
         if (fingerprints[i][idx] == fp) {
            maxCount = Math.max(maxCount, counters[i][idx]);
         }
      }
      return maxCount;
   }

   /**
    * Double hashing: combines two hash values to produce the bucket index for a
    * given row. Each row uses a different linear combination, spreading items
    * across the sketch independently.
    */
   private int index(long hash1, long hash2, int row) {
      long combinedHash = hash1 + (long) row * hash2;
      return (int) (Math.abs(combinedHash) % width);
   }
}
