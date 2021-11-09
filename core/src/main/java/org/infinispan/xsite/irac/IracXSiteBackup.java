package org.infinispan.xsite.irac;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.xsite.XSiteBackup;

/**
 * Extends {@link XSiteBackup} class with logging configuration.
 *
 * @since 14.0
 */
public class IracXSiteBackup extends XSiteBackup {

   private final boolean logExceptions;

   public IracXSiteBackup(String siteName, boolean sync, long timeout, boolean logExceptions) {
      super(siteName, sync, timeout);
      this.logExceptions = logExceptions;
   }

   public boolean logExceptions() {
      return logExceptions;
   }

   public static IracXSiteBackup fromBackupConfiguration(BackupConfiguration backupConfiguration) {
      return new IracXSiteBackup(backupConfiguration.site(), true, backupConfiguration.replicationTimeout(), backupConfiguration.backupFailurePolicy() == BackupFailurePolicy.WARN);
   }
}
