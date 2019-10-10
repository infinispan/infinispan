package org.infinispan.commons.util.concurrent;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Supplier;

import org.infinispan.commons.util.ProcessorInfo;

/**
 * Duplicates a set of counters in a set of stripes, so that multiple threads can increment those counters without too
 * much contention.
 * <p>
 * Callers must first obtain a stripe for the current thread with {@link #stripeForCurrentThread()}, then use {@link
 * #increment(AtomicLongFieldUpdater, Object)} or {@link #add(AtomicLongFieldUpdater, Object, long)} to update one or
 * more counters in that stripe. They must also provide a {@link AtomicLongFieldUpdater} to access a specific counter in
 * the stripe - it should be defined as {@code static final} so that it can be inlined by the JIT.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public final class StripedCounters<T> {
   private static final int STRIPE_COUNT = (int) (Long.highestOneBit(ProcessorInfo.availableProcessors()) << 1);
   private static final int STRIPE_MASK = STRIPE_COUNT - 1;

   @SuppressWarnings("unchecked")
   private final T[] stripes = (T[]) new Object[STRIPE_COUNT];

   public StripedCounters(Supplier<T> stripeSupplier) {
      for (int i = 0; i < stripes.length; i++) {
         stripes[i] = stripeSupplier.get();
      }
   }

   public void increment(AtomicLongFieldUpdater<T> updater, T stripe) {
      updater.getAndIncrement(stripe);
   }

   public void add(AtomicLongFieldUpdater<T> updater, T stripe, long delta) {
      updater.getAndAdd(stripe, delta);
   }

   public long get(AtomicLongFieldUpdater<T> updater) {
      long sum = 0;
      for (T stripe : stripes) {
         sum += updater.get(stripe);
      }
      return sum;
   }

   public void reset(AtomicLongFieldUpdater<T> updater) {
      for (T stripe : stripes) {
         updater.set(stripe, 0);
      }
   }

   public T stripeForCurrentThread() {
      return stripes[threadIndex()];
   }

   private int threadIndex() {
      // Spread the thread id a bit, in case it's always a multiple of 16
      long id = Thread.currentThread().getId();
      id ^= id >>> 7 ^ id >>> 4;
      return (int) (id & STRIPE_MASK);
   }
}
