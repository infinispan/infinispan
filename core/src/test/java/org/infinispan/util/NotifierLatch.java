package org.infinispan.util;

/**
 * A latch that can be open and close. It allows the notification when the some thread is blocking in it.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class NotifierLatch {
   private boolean enabled = false;
   private boolean blocked = false;
   private int disableOnUnblock = 0;

   public final synchronized void startBlocking() {
      this.enabled = true;
   }

   public final synchronized void stopBlocking() {
      this.enabled = false;
      this.disableOnUnblock = 0;
      notifyAll();
   }

   public final synchronized void blockIfNeeded() {
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
      }
   }

   public final synchronized void waitToBlock() throws InterruptedException {
      while (!blocked) {
         wait();
      }
   }

   public synchronized void unblockOnce() {
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
