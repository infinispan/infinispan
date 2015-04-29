package org.infinispan.util.concurrent;

import org.infinispan.commons.api.Lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * A class that handles restarts of components via multiple threads.  Specifically, if a component needs to be restarted
 * and several threads may demand a restart but only one thread should be allowed to restart the component, then use
 * this class.
 * <p/>
 * What this class guarantees is that several threads may come in while a component is being restarted, but they will
 * block until the restart is complete.
 * <p/>
 * This is different from other techniques in that: <ul> <li>A simple compare-and-swap to check whether another thread
 * is already performing a restart will result in the requesting thread returning immediately and potentially attempting
 * to use the resource being restarted.</li> <li>A synchronized method or use of a lock would result in the thread
 * waiting for the restart to complete, but on completion will attempt to restart the component again.</li> </ul> This
 * implementation combines a compare-and-swap to detect a concurrent restart, as well as registering for notification
 * for when the restart completes and then parking the thread if the CAS variable still indicates a restart in progress,
 * and finally deregistering itself in the end.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class SynchronizedRestarter {
   private AtomicBoolean restartInProgress = new AtomicBoolean(false);
   private ConcurrentHashSet<Thread> restartWaiters = new ConcurrentHashSet<Thread>();

   public void restartComponent(Lifecycle component) throws Exception {
      // will only enter this block if no one else is restarting the socket
      // and will atomically set the flag so others won't enter
      if (restartInProgress.compareAndSet(false, true)) {
         try {
            component.stop();
            component.start();
         }
         finally {
            restartInProgress.set(false);
            for (Thread waiter : restartWaiters) {
               try {
                  LockSupport.unpark(waiter);
               }
               catch (Throwable t) {
                  // do nothing; continue notifying the rest
               }
            }
         }
      } else {
         // register interest in being notified after the restart
         restartWaiters.add(Thread.currentThread());
         // check again to ensure the restarting thread hasn't finished, then wait for that thread to finish
         if (restartInProgress.get()) LockSupport.park();
         // de-register interest in notification
         restartWaiters.remove(Thread.currentThread());
      }
   }
}
