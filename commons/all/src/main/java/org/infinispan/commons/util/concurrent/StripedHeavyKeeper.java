package org.infinispan.commons.util.concurrent;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;
import java.util.function.ToLongBiFunction;

import org.infinispan.commons.stat.HeavyKeeper;
import org.infinispan.commons.util.ProcessorInfo;

import com.google.errorprone.annotations.ThreadSafe;

/**
 * A thread-safe, striped wrapper around {@link HeavyKeeper} for concurrent top-k frequency tracking.
 *
 * <p>
 * Partitions updates across cache-line-padded shards, each containing an independent {@code HeavyKeeper} instance.
 * Threads are mapped to shards by thread ID, creating some artificial affinity between shards and threads.
 * </p>
 *
 * <p>
 * {@link #tryAdd} uses a single CAS attempt and drops the update on contention rather than blocking. This is safe because
 * {@code HeavyKeeper} is inherently probabilistic. Hot keys accumulate enough successful updates across their many accesses
 * to remain accurately tracked despite occasional drops.
 * </p>
 *
 * <p>
 * {@link #topKeys()} merges results across all shards, summing counts for keys that appear in multiple shards. Each shard
 * tracks the full {@code k} entries to avoid losing moderately hot keys whose accesses are spread across shards.
 * </p>
 *
 * @param <T> the type of items being tracked
 * @since 16.3
 * @see HeavyKeeper
 */
@ThreadSafe
public final class StripedHeavyKeeper<T> {

   private static final long MIN_PARK_PERIOD_NS = 1_000L;
   private static final long MAX_PARK_PERIOD_NS = 500_000L;

   private static final int MIN_SHARD_COUNT = 8;
   // Ensure power of 2 for the number of shards.
   // Just to make it possible to utilize bitwise operations instead of mod.
   private static final int SHARD_COUNT = Integer.highestOneBit(Math.min(Math.max(ProcessorInfo.availableProcessors(), MIN_SHARD_COUNT), 32) - 1) << 1;
   private static final int MASK = SHARD_COUNT - 1;

   private final Shard<T>[] shards;
   private final int k;

   @SuppressWarnings("unchecked")
   public StripedHeavyKeeper(int k, int width, int depth, double decay, ToLongBiFunction<T, Integer> hash) {
      this.k = k;
      this.shards = new Shard[SHARD_COUNT];
      // This will track k * SHARD_COUNT objects at the end.
      // If we split k by shards, it would require a key to be way hotter in most of the shards.
      // We could falsely drop entries that are slithly less hot but should still make to the top-k.
      for (int i = 0; i < SHARD_COUNT; i++) {
         HeavyKeeper<T> hk = new HeavyKeeper<>(k, width, depth, decay, hash);
         shards[i] = new Shard<>(hk);
      }
   }

   /**
    * Attempts to record an access to the given key without blocking.
    *
    * <p>
    * If the thread's shard lock is available, the key is added to that shard's {@link HeavyKeeper}. If the lock is
    * contended, the update is silently dropped.
    * </p>
    *
    * @param key the item to record
    */
   public void tryAdd(T key) {
      // Utilizing the thread ID will create an artificial locality for the shard information.
      // A thread that is doing frequent work, e.g., a Netty thread associated with a connection, will map to the same shard always.
      int shardIdx = (int) (Thread.currentThread().getId() & MASK);
      Shard<T> shard = shards[shardIdx];

      // HeavyKeeper is inherently a probabilistic structure, so we will be safe to drop some tracks here.
      // In an uncontended scenario, the threads will be more likely to acquire the lock and track the requests.
      // In a contended scenario, there is a high likelihood of uneven access to keys.
      // If we are in a contended scenario, more requests will be to hot keys, so they will inevitably still be recorded
      // as hot, even if we drop some of the tracks.
      if (shard.tryLock()) {
         try {
            shard.hk().add(key);
         } finally {
            shard.unlock();
         }
      }
   }

   /**
    * Returns the top-k most frequent items across all shards.
    *
    * <p>
    * Acquires each shard's lock in sequence, collects top-k entries, and merges by summing counts for keys that appear
    * in multiple shards. This method blocks and is intended for infrequent queries, not the hot path.
    * </p>
    *
    * @return the top-k items sorted by frequency in descending order
    */
   public List<HeavyKeeper.KeyFrequency<T>> topKeys() {
      Map<T, HeavyKeeper.KeyFrequency<T>> aggregated = new HashMap<>();

      for (Shard<T> shard : shards) {
         shard.lock();

         try {
            for (HeavyKeeper.KeyFrequency<T> item : shard.hk().list()) {
               aggregated.merge(item.key(), item, HeavyKeeper.KeyFrequency::sum);
            }
         } finally {
            shard.unlock();
         }
      }

      List<HeavyKeeper.KeyFrequency<T>> result = new ArrayList<>(aggregated.values());
      result.sort(Comparator.reverseOrder());

      if (result.size() > k)
         return result.subList(0, k);

      return result;
   }

   /**
    * Resets all shards, clearing both sketches and top-k maps.
    *
    * <p>
    * Acquires each shard's lock in sequence. Concurrent {@link #tryAdd} calls to already-reset shards may proceed while
    * later shards are still being reset.
    * </p>
    */
   public void reset() {
      for (Shard<T> shard : shards) {
         shard.lock();
         try {
            shard.hk().reset();
         } finally {
            shard.unlock();
         }
      }
   }

   private static class ShardState {

      private static final VarHandle LOCK_HANDLE;

      static {
         try {
            LOCK_HANDLE = MethodHandles.lookup()
                  .findVarHandle(ShardState.class, "lock", int.class);
         } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
         }
      }

      // Lock is kept here while the HeavyKeeper instance is on the subclass.
      // This way we place the attributes in different cache lines.
      // We place all the padding variables between the lock var and the HK instance.
      private volatile int lock;

      private volatile long p1, p2, p3, p4, p5, p6, p7;

      public final boolean tryLock() {
         return LOCK_HANDLE.compareAndSet(this, 0, 1);
      }

      public final void lock() {
         int tries = 0;
         while (!tryLock()) {
            backoff(tries++);
         }
      }

      public final void unlock() {
         // Explicit volatile write is fine to release the lock.
         lock = 0;
      }

      private static void backoff(int count) {
         // We start with spinning wait for 10 times.
         if (count < 10) {
            Thread.onSpinWait();
            return;
         }

         // If we already tried wait spinning 10 times and didn't succeed, we go with thread yield a few times.
         if (count < 15) {
            Thread.yield();
            return;
         }

         // If we still haven't managed to acquire the lock, we start to park the thread a few ns.
         // It could be some other threads are having trouble to be scheduled to release the lock.
         // We bound how large the period can get twice, on the shift and hard maximum.
         // The shift gets up to 1_000 << 9 = 512000, which is slightly more than MAX_PARK.
         long s = Math.min(count - 15, 9);
         long period = Math.min(MIN_PARK_PERIOD_NS << s, MAX_PARK_PERIOD_NS);
         LockSupport.parkNanos(period);
      }
   }

   static final class Shard<T> extends ShardState {
      private final HeavyKeeper<T> hk;

      private volatile int i1;
      private volatile long p9, p10, p11, p12, p13, p14, p15;

      Shard(HeavyKeeper<T> hk) {
         this.hk = hk;
      }

      public HeavyKeeper<T> hk() {
         return hk;
      }
   }
}
