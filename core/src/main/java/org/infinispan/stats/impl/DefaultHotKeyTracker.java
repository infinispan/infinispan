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
   public boolean isEnabled() {
      return true;
   }

   @Override
   public void recordRead(Object key, int segment) {
      // Only register the increment if segment is valid, but always track the key.
      if (TrackerSegments.isValid(segment, readsPerSegment.length))
         readsPerSegment[segment].increment();
      reads.tryAdd(key);
   }

   @Override
   public void recordWrite(Object key, int segment) {
      // Only register the increment if segment is valid, but always track the key.
      if (TrackerSegments.isValid(segment, writesPerSegment.length))
         writesPerSegment[segment].increment();
      writes.tryAdd(key);
   }

   @Override
   public List<HeavyKeeper.KeyFrequency<Object>> getTopReads(int n) {
      return limit(reads.topKeys(), TrackerSegments.requireValidLimit(n));
   }

   @Override
   public List<HeavyKeeper.KeyFrequency<Object>> getTopWrites(int n) {
      return limit(writes.topKeys(), TrackerSegments.requireValidLimit(n));
   }

   private static List<HeavyKeeper.KeyFrequency<Object>> limit(List<HeavyKeeper.KeyFrequency<Object>> keys, int n) {
      return keys.size() <= n ? keys : keys.subList(0, n);
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
      return readsPerSegment[TrackerSegments.requireValid(segment, readsPerSegment.length)].sum();
   }

   @Override
   public long segmentWrites(int segment) {
      return writesPerSegment[TrackerSegments.requireValid(segment, writesPerSegment.length)].sum();
   }

   @Override
   public void reset() {
      // Sketches and counters are reset one after the other, without stopping concurrent recordRead/recordWrite calls.
      // An access that overlaps the reset can therefore remain in one and not the other.
      // This is an intentional design choice.
      // We don't want to implement any form of synchronisation that would cause retention in all cache operations.
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
}
