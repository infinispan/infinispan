package org.infinispan.util.concurrent;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A concurrency structure that invokes a {@link Runnable} when its count reaches zero.
 * <p>
 * Method {@link #increment()} and {@link #decrement()} are available to increase or decrease the counter. When {@link
 * #freeze()} is invoked, no more {@link #increment()} are allowed to be called. It assumes a correct invocation
 * behavior, i.e. {@link #increment()} is invoked before the corresponding {@link #decrement()}.
 * <p>
 * The {@link Runnable} is executed only once.
 *
 * @since 14
 */
public class CountDownRunnable {

   private static final AtomicIntegerFieldUpdater<CountDownRunnable> PENDING_UPDATER = AtomicIntegerFieldUpdater.newUpdater(CountDownRunnable.class, "pending");
   private static final int COMPLETED = -1;
   private static final int READY = 0;

   private final Runnable runnable;
   private volatile int pending = READY;
   private volatile boolean frozen;

   public CountDownRunnable(Runnable runnable) {
      this.runnable = Objects.requireNonNull(runnable);
   }

   public void increment() {
      if (frozen) {
         throw new IllegalStateException();
      }
      PENDING_UPDATER.incrementAndGet(this);
   }

   public void decrement() {
      if (PENDING_UPDATER.decrementAndGet(this) == READY && frozen) {
         tryComplete();
      }
   }

   public int missing() {
      return pending;
   }

   public void freeze() {
      frozen = true;
      tryComplete();
   }

   private void tryComplete() {
      if (PENDING_UPDATER.compareAndSet(this, READY, COMPLETED)) {
         runnable.run();
      }
   }
}
