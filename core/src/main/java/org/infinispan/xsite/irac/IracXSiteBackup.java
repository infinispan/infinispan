package org.infinispan.xsite.irac;

import org.infinispan.util.ExponentialBackOff;
import org.infinispan.xsite.XSiteBackup;

import net.jcip.annotations.GuardedBy;

/**
 * Extends {@link XSiteBackup} class with logging configuration.
 *
 * @since 14.0
 */
public class IracXSiteBackup extends XSiteBackup implements Runnable {

   private final boolean logExceptions;
   private final short index;
   @GuardedBy("this")
   private boolean backOffEnabled;
   @GuardedBy("this")
   private ExponentialBackOff backOff;
   @GuardedBy("this")
   private Runnable afterBackOff = () -> {};

   public IracXSiteBackup(String siteName, boolean sync, long timeout, boolean logExceptions, short index) {
      super(siteName, sync, timeout);
      assert index >= 0;
      this.logExceptions = logExceptions;
      this.backOff = ExponentialBackOff.NO_OP;
      this.backOffEnabled = false;
      this.index = index;
   }

   public boolean logExceptions() {
      return logExceptions;
   }

   public synchronized void enableBackOff() {
      if (backOffEnabled) return;

      backOffEnabled = true;
      backOff.asyncBackOff().thenRun(this);
   }

   synchronized void useBackOff(ExponentialBackOff backOff, Runnable after) {
      this.backOff = backOff;
      this.afterBackOff = after;
   }

   public synchronized boolean isBackOffEnabled() {
      return backOffEnabled;
   }

   synchronized void resetBackOff() {
      backOffEnabled = false;
      backOff.reset();
      afterBackOff.run();
   }

   @Override
   public synchronized void run() {
      backOffEnabled = false;
      afterBackOff.run();
   }

   @Override
   public String toString() {
      return super.toString() + (isBackOffEnabled() ? " [backoff-enabled]" : "");
   }

   public int siteIndex() {
      return index;
   }
}
