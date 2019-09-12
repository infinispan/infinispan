package org.infinispan.util;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A latch that can be open and close. It allows the notification when the some thread is blocking in it.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class NotifierLatch {
   private static final Log log = LogFactory.getLog(NotifierLatch.class);

   private final String name;
   private boolean enabled = false;
   private boolean blocked = false;
   private int disableOnUnblock = 0;

   public NotifierLatch(String name) {
      this.name = name;
   }

   public final synchronized void startBlocking() {
      log.tracef("Start blocking %s", name);
      this.enabled = true;
   }

   public final synchronized void stopBlocking() {
      log.tracef("Stop blocking %s", name);
      this.enabled = false;
      this.disableOnUnblock = 0;
      notifyAll();
   }

   public final synchronized void blockIfNeeded() {
      log.tracef("Blocking on %s", name);
      blocked = true;
      notifyAll();
      try {
         while (enabled) {
            try {
               wait();
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               return;
            }
         }
      } finally {
         blocked = false;
         if (disableOnUnblock > 0 && --disableOnUnblock == 0) {
            enabled = true;
         }
         log.tracef("Resuming on %s", name);
      }
   }

   public final synchronized void waitToBlock() throws InterruptedException {
      log.tracef("Waiting for another thread to block on %s", name);
      while (!blocked) {
         wait();
      }
   }

   public synchronized void unblockOnce() {
      log.tracef("Unblocking once %s", name);
      enabled = false;
      disableOnUnblock++;
      notifyAll();
   }

   public void waitToBlockAndUnblockOnce() throws InterruptedException {
      waitToBlock();
      unblockOnce();
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("NotifierLatch{");
      sb.append("enabled=").append(enabled);
      sb.append(", blocked=").append(blocked);
      sb.append(", disableOnUnblock=").append(disableOnUnblock);
      sb.append('}');
      return sb.toString();
   }
}
