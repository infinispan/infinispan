package org.infinispan.commons.util;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * A notification barrier that makes threads waiting until they are notified.
 * <p>
 * The semantic is similar as {@code new CountDownLatch(1)} but it allows listeners to be added. The listeners can be
 * any class and they are invoked when the method {@link #fireListener()} is invoked. Threads invoking {@link
 * #await(long, TimeUnit)} will be unblocked too.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class Notifier<T> {

   private static final int NOTIFIED = 0;
   private static final int WAITING = 1;

   //it allows to perform some action before/after the listener invocation or adding some information to the listener.
   private final Invoker<T> invoker;
   private final Queue<T> listeners;
   private final Sync sync;

   /**
    * Constructs a new instance.
    *
    * @param invoker the {@link org.infinispan.commons.util.Notifier.Invoker} is invoked with the listener to be
    *                notified.
    * @throws NullPointerException if {@code invoker} is null.
    */
   public Notifier(Invoker<T> invoker) {
      Objects.requireNonNull(invoker, "Invoker must be non-null");
      this.invoker = invoker;
      this.listeners = new ConcurrentLinkedQueue<>();
      this.sync = new Sync();
   }

   /**
    * Adds a listener.
    * <p>
    * If the method {@link #fireListener()} was already invoked, the listener could be invoked by the invocation thread.
    * The {@code listener} is invoked only once.
    *
    * @param listener the listener to invoke.
    * @throws NullPointerException if the {@code listener} is null.
    */
   public void add(T listener) {
      Objects.requireNonNull(listener, "Listener must be non-null");
      listeners.add(listener);
      if (sync.isNotified()) {
         trigger();
      }
   }

   /**
    * Makes the thread waiting until {@link #fireListener()} is invoked or the specified waiting {@code time} elapses.
    *
    * @param time the maximum time to wait
    * @param unit the time unit of the {@code time}
    * @return {@code true} if another thread invoked {@link #fireListener()}, {@code false} otherwise.
    * @throws InterruptedException if the current thread is interrupted while waiting
    * @throws NullPointerException if {@code unit} is null.
    */
   public boolean await(long time, TimeUnit unit) throws InterruptedException {
      Objects.requireNonNull(unit, "Time unit must be non-null");
      return sync.tryAcquireSharedNanos(1, unit.toNanos(time));
   }

   /**
    * Unblocks all the waiting threads and notifies the listeners.
    *
    * If this method was previously invoked, then nothing happens.
    */
   public void fireListener() {
      if (!sync.isNotified() && sync.releaseShared(1)) {
         trigger();
      }
   }

   private void trigger() {
      T listener;
      while ((listener = listeners.poll()) != null) {
         invoker.invoke(listener);
      }
   }

   public interface Invoker<T1> {

      /**
       * It invokes the {@code listener} received as argument.
       *
       * @param listener the listener to be invoked. The argument is non-null.
       */
      void invoke(T1 listener);
   }

   private static class Sync extends AbstractQueuedSynchronizer {

      public Sync() {
         setState(WAITING);
      }

      public boolean isNotified() {
         return getState() == NOTIFIED;
      }

      @Override
      protected boolean tryReleaseShared(int ignored) {
         return compareAndSetState(WAITING, NOTIFIED);
      }

      @Override
      protected int tryAcquireShared(int ignored) {
         return getState() == NOTIFIED ? 1 : -1;
      }
   }
}
