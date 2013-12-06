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

   public final synchronized void startBlocking() {
      this.enabled = true;
   }

   public final synchronized void stopBlocking() {
      this.enabled = false;
      notifyAll();
   }

   public final synchronized void blockIfNeeded() {
      blocked = true;
      notifyAll();
      while (enabled) {
         try {
            wait();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
         }
      }
   }

   public final synchronized void waitToBlock() throws InterruptedException {
      while (!blocked) {
         wait();
      }
   }

}
