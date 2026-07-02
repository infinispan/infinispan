package org.infinispan.stats.impl;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ToLongBiFunction;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.stat.HeavyKeeper;
import org.infinispan.commons.util.concurrent.StripedHeavyKeeper;
import org.infinispan.configuration.cache.HotKeysConfiguration;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.stats.HotKeyTracker;
import org.infinispan.util.logging.Log;

/**
 * The {@link HotKeyTracker} used when hot key tracking is enabled.
 * <p>
 * Reads and writes are tracked independently, each by a {@link StripedHeavyKeeper}. Recording never blocks; under
 * contention some accesses are not counted in the top keys, while the per-segment totals count every access.
 * </p>
 *
 * @since 16.4
 * @author José Bolina
 */
@Scope(Scopes.NAMED_CACHE)
public final class DefaultHotKeyTracker implements HotKeyTracker {

   private static final ToLongBiFunction<Object, Integer> HASH = MurmurHash3.getInstance()::MurmurHash3_x64_64;

   // Each key gets 4 sketch columns per row, bounded below so that small k still has room to separate keys.
   private static final int WIDTH_PER_KEY = 4;
   private static final int MIN_WIDTH = 128;

   // The configuration caps k, so the widest sketch is 512 * 4 = 2048 columns.
   // We'll have at most 32 stripes of HeavyKeepers, each with 2 long[] (fingerprints and counters) per row.
   // The total size of each StripedHK is: 32 * 2 * Long.BYTES * WIDTH * DEPTH.
   // That is ~458 KB at the minimum width and ~7.3 MB at the maximum width, with two instances per cache.
   private static final int MAX_WIDTH = HotKeysConfiguration.MAX_TOP_K * WIDTH_PER_KEY;

   // This means 7 rows of idependent hash function.
   // The probability of failures becomes e^-7 ~= 0.00091.
   private static final int DEPTH = 7;

   // Probability of decay is given as P_d = b^-C; where C = the current value in the counter.
   // The paper has an example value of b = 1.08, then P_d = 1.08^-1 = 1/1.08 ~= 0.925
   private static final double DECAY = 0.925;

   private final StripedHeavyKeeper<Object> reads;
   private final StripedHeavyKeeper<Object> writes;

   private final LongAdder[] readsPerSegment;
   private final LongAdder[] writesPerSegment;

   /**
    * Creates a tracker that reports up to {@code k} keys for reads and for writes, and counts accesses
    * per segment.
    *
    * @param k           the number of keys to track per operation type, between 1 and
    *                    {@link HotKeysConfiguration#MAX_TOP_K}
    * @param numSegments the number of segments of the cache
    * @throws org.infinispan.commons.CacheConfigurationException if {@code k} is out of range
    */
   public DefaultHotKeyTracker(int k, int numSegments) {
      // The configuration validates k, but this class can be created directly, so it enforces the same limits.
      if (k < 1 || k > HotKeysConfiguration.MAX_TOP_K)
         throw Log.CONFIG.hotKeysTopKOutOfRange(k, HotKeysConfiguration.MAX_TOP_K);

      int width = estimateWidth(k);
      this.reads = new StripedHeavyKeeper<>(k, width, DEPTH, DECAY, HASH);
      this.writes = new StripedHeavyKeeper<>(k, width, DEPTH, DECAY, HASH);
      this.readsPerSegment = adders(numSegments);
      this.writesPerSegment = adders(numSegments);
   }

   @Override
   public void recordRead(Object key, int segment) {
      // Only register the increment if segment is valid, but always track the key.
      if (isValidSegment(readsPerSegment, segment))
         readsPerSegment[segment].increment();
      reads.tryAdd(key);
   }

   @Override
   public void recordWrite(Object key, int segment) {
      // Only register the increment if segment is valid, but always track the key.
      if (isValidSegment(writesPerSegment, segment))
         writesPerSegment[segment].increment();
      writes.tryAdd(key);
   }

   @Override
   public List<HeavyKeeper.KeyFrequency<Object>> getTopReads(int n) {
      List<HeavyKeeper.KeyFrequency<Object>> res = reads.topKeys();
      return res.size() <= n ? res : res.subList(0, n);
   }

   @Override
   public List<HeavyKeeper.KeyFrequency<Object>> getTopWrites(int n) {
      List<HeavyKeeper.KeyFrequency<Object>> res = writes.topKeys();
      return res.size() <= n ? res : res.subList(0, n);
   }

   @Override
   public long totalReads() {
      return sum(readsPerSegment);
   }

   @Override
   public long totalWrites() {
      return sum(writesPerSegment);
   }

   @Override
   public long segmentReads(int segment) {
      return readsPerSegment[requireValidSegment(readsPerSegment, segment)].sum();
   }

   @Override
   public long segmentWrites(int segment) {
      return writesPerSegment[requireValidSegment(writesPerSegment, segment)].sum();
   }

   @Override
   public void reset() {
      reads.reset();
      writes.reset();

      reset(readsPerSegment);
      reset(writesPerSegment);
   }

   /**
    * Computes the sketch width for tracking {@code k} keys.
    *
    * @param k the number of keys to track, between 1 and {@link HotKeysConfiguration#MAX_TOP_K}
    * @return a power of two between {@code MIN_WIDTH} and {@code MAX_WIDTH}
    */
   private static int estimateWidth(int k) {
      // Give the sketch some room relative to k.
      // Since k is bounded, the result never exceeds MAX_WIDTH.
      int target = Math.max(MIN_WIDTH, k * WIDTH_PER_KEY);

      // Round up to the next power of two.
      return Integer.highestOneBit(target - 1) << 1;
   }

   /**
    * Adds up a set of counters.
    *
    * @param adders the counters to add up
    * @return the sum of all counters
    */
   private static long sum(LongAdder[] adders) {
      long sum = 0;
      for (LongAdder adder : adders) {
         sum += adder.sum();
      }
      return sum;
   }

   /**
    * Sets every counter back to zero.
    *
    * @param adders the counters to reset
    */
   private static void reset(LongAdder[] adders) {
      for (LongAdder adder : adders) {
         adder.reset();
      }
   }

   /**
    * Creates a set of counters starting at zero.
    *
    * @param size the number of counters
    * @return a new array of {@code size} counters
    */
   private static LongAdder[] adders(int size) {
      LongAdder[] adders = new LongAdder[size];
      for (int i = 0; i < size; i++) {
         adders[i] = new LongAdder();
      }
      return adders;
   }

   /**
    * Checks whether a segment has a counter in the given array.
    *
    * @param adders  the per-segment counters
    * @param segment the segment to check
    * @return {@code true} if {@code segment} is between zero (inclusive) and the number of segments (exclusive)
    */
   private static boolean isValidSegment(LongAdder[] adders, int segment) {
      return segment >= 0 && segment < adders.length;
   }

   /**
    * Validates a segment passed by a caller querying the per-segment counters.
    *
    * @param adders  the per-segment counters
    * @param segment the segment to validate
    * @return the same {@code segment}, so the call can be used as an array index
    * @throws IllegalArgumentException if {@code segment} is negative or not lower than the number of segments
    */
   private static int requireValidSegment(LongAdder[] adders, int segment) {
      if (!isValidSegment(adders, segment))
         throw new IllegalArgumentException(String.format("Segment %d is out of range [0, %d]", segment, adders.length));
      return segment;
   }
}
