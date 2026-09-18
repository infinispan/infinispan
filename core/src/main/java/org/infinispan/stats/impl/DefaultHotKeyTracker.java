package org.infinispan.stats.impl;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ToLongBiFunction;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.stat.HeavyKeeper;
import org.infinispan.commons.util.concurrent.StripedHeavyKeeper;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.stats.HotKeyTracker;
import org.infinispan.util.logging.Log;

@Scope(Scopes.NAMED_CACHE)
public final class DefaultHotKeyTracker implements HotKeyTracker {

   private static final ToLongBiFunction<Object, Integer> HASH = MurmurHash3.getInstance()::MurmurHash3_x64_64;

   // We'll have at most 32 stripes of HeavyKeepers.
   // The HK instance will have 2 long[]: 2 * Long.Bytes.
   // The total size of each StripedHK would be given by: 32 * 2 * Long.Bytes * WIDTH * DEPTH.
   // With the current values, each Stripped has a size of ~458Kb, in the worst case, ~6Mb with the max bound.
   private static final int MIN_WIDTH = 128;
   private static final int MAX_WIDTH = 2048;

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

   public DefaultHotKeyTracker(int k, int numSegments) {
      int width = estimateWidth(k);
      this.reads = new StripedHeavyKeeper<>(k, width, DEPTH, DECAY, HASH);
      this.writes = new StripedHeavyKeeper<>(k, width, DEPTH, DECAY, HASH);
      this.readsPerSegment = adders(numSegments);
      this.writesPerSegment = adders(numSegments);
   }

   @Override
   public void recordRead(Object key, int segment) {
      readsPerSegment[segment].increment();
      reads.tryAdd(key);
   }

   @Override
   public void recordWrite(Object key, int segment) {
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
      return readsPerSegment[segment].sum();
   }

   @Override
   public long segmentWrites(int segment) {
      return writesPerSegment[segment].sum();
   }

   @Override
   public void reset() {
      reads.reset();
      writes.reset();

      reset(readsPerSegment);
      reset(writesPerSegment);
   }

   private static int estimateWidth(int k) {
      // The idea is to allocate a wide enough sketch to hold the K values without making it too ineffective.
      // We give some room in the relation of k and the sketch size, but we bound it to a maximum value.
      int target = Math.max(MIN_WIDTH, k * 4);

      if (target > MAX_WIDTH) {
         Log.CONTAINER.topKeysToTrackTooLarge("", k);
         target = MAX_WIDTH;
      }

      // Return the new power of two for the given target.
      return Integer.highestOneBit(target - 1) << 1;
   }

   private static long sum(LongAdder[] adders) {
      long sum = 0;
      for (LongAdder adder : adders) {
         sum += adder.sum();
      }
      return sum;
   }

   private static void reset(LongAdder[] adders) {
      for (LongAdder adder : adders) {
         adder.reset();
      }
   }

   private static LongAdder[] adders(int size) {
      LongAdder[] adders = new LongAdder[size];
      for (int i = 0; i < size; i++) {
         adders[i] = new LongAdder();
      }
      return adders;
   }
}
