package org.infinispan.xsite.irac;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
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

   @GuardedBy("this")
   private ExponentialBackOff backOff;

   @GuardedBy("this")
   private boolean backOffEnabled;

   @GuardedBy("this")
   private Runnable afterBackOff = () -> {};

   public IracXSiteBackup(String siteName, boolean sync, long timeout, boolean logExceptions) {
      super(siteName, sync, timeout);
      this.logExceptions = logExceptions;
      this.backOff = ExponentialBackOff.NO_OP;
      this.backOffEnabled = false;
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

   public static IracXSiteBackup fromBackupConfiguration(BackupConfiguration backupConfiguration) {
      return new IracXSiteBackup(backupConfiguration.site(), true, backupConfiguration.replicationTimeout(), backupConfiguration.backupFailurePolicy() == BackupFailurePolicy.WARN);
   }
}
