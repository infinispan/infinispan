package org.infinispan.util.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * Allows threads to watch a variable and be notified when the state of the variable reaches a specific value.
 * <p/>
 * E.g.,
 * <p/>
 * ValueNotifier v = new ValueNotifier(0);
 * <p/>
 * Thread1:
 * <p/>
 * v.setValue(10);
 * <p/>
 * Thread2:
 * <p/>
 * v.awaitValue(5); // will block until another thread sets the value to 5
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class WatchableValue extends AbstractQueuedSynchronizer {
   private static final long serialVersionUID = 1744280161777661090l;

   public WatchableValue(int initValue) {
      setState(initValue);
   }

   @Override
   public final int tryAcquireShared(int value) {
      // return 1 if we allow the requestor to proceed, -1 if we want the requestor to block.
      return getState() == value ? 1 : -1;
   }

   @Override
   public final boolean tryReleaseShared(int state) {
      // used as a mechanism to set the state of the Sync.
      setState(state);
      return true;
   }

   public final void setValue(int value) {
      // do not use setState() directly since this won't notify parked threads.
      releaseShared(value);
   }

   public final void awaitValue(int value) throws InterruptedException {
      acquireSharedInterruptibly(value);
   }

   public final boolean awaitValue(int value, long time, TimeUnit unit) throws InterruptedException {
      return tryAcquireSharedNanos(value, unit.toNanos(time));
   }

   public int getValue() {
      return getState();
   }
}
