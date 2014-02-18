package org.infinispan.configuration.cache;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class BackupConfiguration {

   private final String site;
   private final BackupStrategy strategy;
   private long timeout;
   private final BackupFailurePolicy backupFailurePolicy;
   private final String failurePolicyClass;
   private final boolean useTwoPhaseCommit;
   private final TakeOfflineConfiguration takeOfflineConfiguration;
   private final XSiteStateTransferConfiguration stateTransferConfiguration;
   private final boolean enabled;

   public BackupConfiguration(String site, BackupStrategy strategy, long timeout, BackupFailurePolicy backupFailurePolicy,
                              String failurePolicyClass, boolean useTwoPhaseCommit, TakeOfflineConfiguration takeOfflineConfiguration, XSiteStateTransferConfiguration stateTransferConfiguration, boolean enabled) {
      this.site = site;
      this.strategy = strategy;
      this.timeout = timeout;
      this.backupFailurePolicy = backupFailurePolicy;
      this.failurePolicyClass = failurePolicyClass;
      this.useTwoPhaseCommit = useTwoPhaseCommit;
      this.takeOfflineConfiguration = takeOfflineConfiguration;
      this.stateTransferConfiguration = stateTransferConfiguration;
      this.enabled = enabled;
   }

   /**
    * Returns the name of the site where this cache backups its data.
    */
   public String site() {
      return site;
   }

   /**
    * How does the backup happen: sync or async.
    */
   public BackupStrategy strategy() {
      return strategy;
   }

   public TakeOfflineConfiguration takeOffline() {
      return takeOfflineConfiguration;
   }

   /**
    * If the failure policy is set to {@link BackupFailurePolicy#CUSTOM} then the failurePolicyClass is required and
    * should return the fully qualified name of a class implementing {@link org.infinispan.xsite.CustomFailurePolicy}
    */
   public String failurePolicyClass() {
      return failurePolicyClass;
   }

   public boolean isAsyncBackup() {
      return strategy() == BackupStrategy.ASYNC;
   }

   public long replicationTimeout() {
      return timeout;
   }

   public BackupFailurePolicy backupFailurePolicy() {
      return backupFailurePolicy;
   }

   public enum BackupStrategy {
      SYNC, ASYNC
   }

   public boolean isTwoPhaseCommit() {
      return useTwoPhaseCommit;
   }

   /**
    * @see BackupConfigurationBuilder#enabled(boolean).
    */
   public boolean enabled() {
      return enabled;
   }

   public XSiteStateTransferConfiguration stateTransfer() {
      return stateTransferConfiguration;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof BackupConfiguration)) return false;

      BackupConfiguration that = (BackupConfiguration) o;

      if (timeout != that.timeout) return false;
      if (backupFailurePolicy != that.backupFailurePolicy) return false;
      if (failurePolicyClass != null ? !failurePolicyClass.equals(that.failurePolicyClass) : that.failurePolicyClass != null)
         return false;
      if (site != null ? !site.equals(that.site) : that.site != null) return false;
      if (useTwoPhaseCommit != that.useTwoPhaseCommit) return false;
      if (strategy != that.strategy) return false;
      if (enabled != that.enabled) return false;
      if (stateTransferConfiguration != null ?
            !stateTransferConfiguration.equals(that.stateTransferConfiguration) :
            that.stateTransferConfiguration != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = site != null ? site.hashCode() : 0;
      result = 31 * result + (strategy != null ? strategy.hashCode() : 0);
      result = 31 * result + (int) (timeout ^ (timeout >>> 32));
      result = 31 * result + (backupFailurePolicy != null ? backupFailurePolicy.hashCode() : 0);
      result = 31 * result + (failurePolicyClass != null ? failurePolicyClass.hashCode() : 0);
      result = 31 * result + (stateTransferConfiguration != null ? stateTransferConfiguration.hashCode() : 0);
      result = 31 * result + (useTwoPhaseCommit ? 1 : 0);
      return result;
   }

   @Override
   public String toString() {
      return "BackupConfiguration{" +
            "site='" + site + '\'' +
            ", strategy=" + strategy +
            ", timeout=" + timeout +
            ", useTwoPhaseCommit=" + useTwoPhaseCommit +
            ", backupFailurePolicy=" + backupFailurePolicy +
            ", failurePolicyClass='" + failurePolicyClass + '\'' +
            ", stateTransferConfiguration=" + stateTransferConfiguration +
            ", enabled='" + enabled + '\'' +
            '}';
   }
}
