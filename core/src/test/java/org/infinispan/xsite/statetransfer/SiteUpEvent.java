package org.infinispan.xsite.statetransfer;

import java.util.Collection;

import org.testng.AssertJUnit;

/**
 * Controls and intercept site up events.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
public class SiteUpEvent {

   private static final long TIMEOUT_MILLIS = 30000;

   private Collection<String> sites;
   private Runnable runnable;

   public synchronized void receive(Collection<String> sites, Runnable runnable) {
      assert this.runnable == null;
      assert this.sites == null;
      this.sites = sites;
      this.runnable = runnable;
      this.notifyAll();
   }

   public synchronized Collection<String> waitForEvent() throws InterruptedException {
      long endMillis = System.currentTimeMillis() + TIMEOUT_MILLIS;
      long waitMillis;
      while (sites == null && (waitMillis = timeLeft(endMillis)) > 0) {
         this.wait(waitMillis);
      }
      Collection<String> sites = this.sites;
      this.sites = null;
      AssertJUnit.assertNotNull(sites);
      return sites;
   }

   public void continueRunnable() {
      Runnable runnable;
      synchronized (this) {
         runnable = this.runnable;
         this.runnable = null;
      }
      if (runnable != null) {
         runnable.run();
      }
   }

   private long timeLeft(long endMillis) {
      return endMillis - System.currentTimeMillis();
   }
}
