package org.infinispan.commons.util.concurrent;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
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
 * {@link #topKeys()} merges results across all shards. The candidates are the keys in any shard's top-k, and each candidate
 * is reported with its estimated count summed over every shard, including shards where it is not in the top-k. A key with
 * more than {@code 1/(k+1)} of all recorded accesses has more than that share in at least one shard, so that shard reports
 * it unless its sketch underestimates the key. Hash collisions and counter decay make such underestimates possible, though
 * rare for frequent keys, so the inclusion is likely but not guaranteed.
 * </p>
 *
 * <p>
 * Keys below that share are reported only when some shard holds them in its own top-k. A key whose accesses are spread
 * across shards that each see other keys more often is left out, even when its total would rank it in the global top-k.
 * This is intentional, see {@link #topKeys()}.
 * </p>
 *
 * @param <T> the type of items being tracked
 * @since 16.4
 * @see HeavyKeeper
 * @author José Bolina
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

   /**
    * Creates a keeper whose shards each track up to {@code k} items with their own sketch of the given dimensions.
    * <p>
    * Memory grows with the shard count: every shard holds {@code 2 * width * depth} longs and up to {@code k}
    * top-k entries. The shard count is fixed per JVM, between 8 and 32, from the available processors.
    * </p>
    *
    * @param k     the number of items to track and report, greater than zero
    * @param width the number of columns in each sketch row
    * @param depth the number of sketch rows
    * @param decay the decay base for colliding counters, strictly between 0 and 1
    * @param hash  hashes an item with a seed; equal items must produce equal hashes for the same seed
    * @throws IllegalArgumentException if {@code k} is not greater than zero, or {@code decay} is not strictly between
    *                                  0 and 1
    */
   @SuppressWarnings("unchecked")
   public StripedHeavyKeeper(int k, int width, int depth, double decay, ToLongBiFunction<T, Integer> hash) {
      if (k <= 0)
         throw new IllegalArgumentException(String.format("The number of items to track must be greater than zero, but was %d", k));

      this.k = k;
      this.shards = new Shard[SHARD_COUNT];
      // Every shard tracks the full k, so the keeper holds up to k * SHARD_COUNT candidates in total.
      // Splitting k across shards would make each shard track only its k / SHARD_COUNT hottest keys, so a key would
      // have to be hot in most shards to survive, dropping keys that belong in the global top-k.
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
    * Returns the most frequent items across all shards.
    *
    * <p>
    * A key with more than {@code 1/(k+1)} of the accesses recorded since the last reset is very likely included, with
    * its count from every shard; it can be missed only when the sketches underestimate it. Other keys are included when
    * some shard tracks them in its own top-k. Counts are estimates and never exceed the number of recorded accesses.
    * Accesses dropped by {@link #tryAdd} on contention are not counted.
    * </p>
    *
    * <p>
    * The result is not the exact global top-k. A key below the {@code 1/(k+1)} share that is missing from every shard's
    * top-k is never a candidate, even when its summed count would rank it among the {@code k} most frequent keys.
    * </p>
    *
    * <p>
    * This method locks each shard in turn and is intended for infrequent queries, not the hot path. Updates that
    * happen while it runs may or may not be included.
    * </p>
    *
    * @return at most {@code k} items sorted by count in descending order
    */
   public List<HeavyKeeper.KeyFrequency<T>> topKeys() {
      // Step 1: collect the candidates, the union of every shard's top-k.
      //
      // Pigeonhole principle.
      // A key with more than N/(k+1) of all N accesses has more than N_i/(k+1) of the N_i accesses in some shard i.
      // A shard tracking k keys always holds such a key in its top-k, unless its sketch underestimates it.
      // So the union contains every key above the threshold.
      //
      // This step intentionally drops keys below the threshold that are in no shard's top-k.
      // For example, with k = 1, shard A sees a:101 and x:100, and shard B sees b:101 and x:100.
      // x has 200 accesses in total, more than a or b, but each shard lists only its local key, so x is never a candidate.
      // 200 is below the 402/2 threshold, so the pigeonhole principle still holds.
      //
      // This is a trade-off.
      // Only the sketches know about keys outside the shards' lists, and a sketch can be queried for a key but cannot enumerate its keys.
      // Keeping more candidates per shard (say 2k) would shrink the gap but not close it, and would cost memory on every shard and time on every query.
      // A key hot enough to matter is accessed by many threads, so its accesses spread over every shard and the pigeonhole principle covers it.
      Set<T> candidates = new LinkedHashSet<>();
      for (Shard<T> shard : shards) {
         shard.lock();
         try {
            for (HeavyKeeper.KeyFrequency<T> item : shard.hk().list()) {
               candidates.add(item.key());
            }
         } finally {
            shard.unlock();
         }
      }

      // Step 2: count every candidate in every shard.
      //
      // A candidate missing from a shard's top-k may still have accesses in that shard's sketch.
      // Summing only the listed counts would undercount keys spread across threads, so each shard's sketch is queried for every candidate.
      // This needs a second lock per shard.
      // The candidates are only known after visiting every shard, and holding all locks at once would drop every concurrent tryAdd for the whole query.
      long[] counts = new long[candidates.size()];
      for (Shard<T> shard : shards) {
         shard.lock();
         try {
            int i = 0;
            for (T key : candidates) {
               counts[i++] += shard.hk().getCount(key);
            }
         } finally {
            shard.unlock();
         }
      }

      // Step 3: rank the candidates by their summed count and keep the k most frequent.
      // KeyFrequency's natural order is ascending by count, so the reverse order sorts the hottest first.
      List<HeavyKeeper.KeyFrequency<T>> result = new ArrayList<>(candidates.size());
      int i = 0;
      for (T key : candidates) {
         result.add(new HeavyKeeper.KeyFrequency<>(key, counts[i++]));
      }
      result.sort(Comparator.comparingLong(HeavyKeeper.KeyFrequency<T>::count).reversed());

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

   /**
    * The lock half of a shard. It lives in a superclass so that padding separates the lock from the
    * {@link HeavyKeeper} reference and threads spinning on one shard's lock do not invalidate another shard's
    * cache line.
    */
   private static class ShardState {

      // Bit 0 of the lock word is set while a thread holds the lock.
      private static final int HELD = 1;

      // The remaining bits count the threads waiting in the lock, in units of WAITER.
      // Keeping the count in the lock word avoids an extra field.
      private static final int WAITER = 2;

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

      /**
       * Acquires the lock if it is free, without waiting.
       *
       * @return {@code true} if the lock was acquired, {@code false} if another thread holds it
       */
      public final boolean tryLock() {
         // Succeeds only when the whole word is zero, i.e., not held and no waiters.
         // This effectively means that we give priority to writers, and they are acquiring uniquely.
         return LOCK_HANDLE.compareAndSet(this, 0, HELD);
      }

      /**
       * Acquires the lock, waiting until it is free. Intended for the query path only.
       *
       * <p>
       * While this method waits, {@link #tryLock()} fails, so the wait lasts at most until the current holder
       * releases the lock. Threads waiting here at the same time compete with each other without ordering.
       * </p>
       */
      public final void lock() {
         // Register first as a waiter, giving priority over tryAcquire operations.
         LOCK_HANDLE.getAndAdd(this, WAITER);
         int tries = 0;
         while (true) {
            int state = (int) LOCK_HANDLE.getVolatile(this);
            // Take the lock and drop the registration in one step, so no update slips in between.
            if ((state & HELD) == 0 && LOCK_HANDLE.compareAndSet(this, state, state - WAITER + HELD))
               return;
            backoff(tries++);
         }
      }

      /**
       * Releases the lock. Must only be called by the thread holding it.
       */
      public final void unlock() {
         // Use an atomic operation instead of plain volatile write.
         // It guarantees the changes made by waiters are still kept.
         LOCK_HANDLE.getAndAdd(this, -HELD);
      }

      /**
       * Waits a little before the next attempt to acquire the lock, waiting longer as attempts accumulate:
       * first spinning, then yielding, then parking for up to {@code MAX_PARK_PERIOD_NS}.
       *
       * @param count the number of failed attempts so far
       */
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

   /**
    * One shard: a {@link HeavyKeeper} guarded by the lock inherited from {@link ShardState}.
    *
    * @param <T> the type of items being tracked
    */
   static final class Shard<T> extends ShardState {
      private final HeavyKeeper<T> hk;

      private volatile int i1;
      private volatile long p9, p10, p11, p12, p13, p14, p15;

      Shard(HeavyKeeper<T> hk) {
         this.hk = hk;
      }

      /**
       * Returns this shard's keeper. Callers must hold the shard's lock while using it.
       *
       * @return the keeper owned by this shard
       */
      public HeavyKeeper<T> hk() {
         return hk;
      }
   }
}
