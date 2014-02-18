package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.CacheConfigurationException;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class BackupConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<BackupConfiguration> {

   private String site;

   private BackupConfiguration.BackupStrategy strategy = BackupConfiguration.BackupStrategy.ASYNC;

   private long replicationTimeout = 10000;

   private BackupFailurePolicy backupFailurePolicy = BackupFailurePolicy.WARN;

   private String failurePolicyClass;

   private boolean useTwoPhaseCommit = false;
   
   private TakeOfflineConfigurationBuilder takeOfflineBuilder;

   private boolean enabled = true;

   private XSiteStateTransferConfigurationBuilder stateTransferBuilder;

   public BackupConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      takeOfflineBuilder = new TakeOfflineConfigurationBuilder(builder, this);
      this.stateTransferBuilder = new XSiteStateTransferConfigurationBuilder(builder, this);
   }

   /**
    * @param site The name of the site where this cache backups. Must be a valid name, i.e. a site defined in the
    *             global config.
    */
   public BackupConfigurationBuilder site(String site) {
      this.site = site;
      return this;
   }

   /**
    * @see #site(String)
    */
   public String site() {
      return this.site;
   }

   /**
    * If the failure policy is set to {@link BackupFailurePolicy#CUSTOM} then the failurePolicyClass is required and
    * should return the fully qualified name of a class implementing {@link org.infinispan.xsite.CustomFailurePolicy}
    */
   public String failurePolicyClass() {
      return failurePolicyClass;
   }

   /**
    * @see #failurePolicyClass()
    */
   public BackupConfigurationBuilder failurePolicyClass(String failurePolicy) {
      this.failurePolicyClass = failurePolicy;
      return this;
   }

   /**
    * Timeout(millis) used for replicating calls to other sites.
    */
   public BackupConfigurationBuilder replicationTimeout(long replicationTimeout) {
      this.replicationTimeout = replicationTimeout;
      return this;
   }

   /**
    * @see {@link #replicationTimeout(long)}
    */
   public long replicationTimeout() {
      return replicationTimeout;
   }

   /**
    * Sets the strategy used for backing up data: sync or async. If not specified defaults
    * to {@link org.infinispan.configuration.cache.BackupConfiguration.BackupStrategy#ASYNC}.
    */
   public BackupConfigurationBuilder strategy(BackupConfiguration.BackupStrategy strategy) {
      this.strategy = strategy;
      return this;
   }

   /**
    * @see #strategy()
    */
   public BackupConfiguration.BackupStrategy strategy() {
      return strategy;
   }

   public TakeOfflineConfigurationBuilder takeOffline() {
      return takeOfflineBuilder;
   }

   /**
    * Configures how the system behaves when the backup call fails. Only applies to sync backups.
    * The default values is  {@link org.infinispan.configuration.cache.BackupFailurePolicy#WARN}
    */
   public BackupConfigurationBuilder backupFailurePolicy(BackupFailurePolicy backupFailurePolicy) {
      this.backupFailurePolicy = backupFailurePolicy;
      return this;
   }

   /**
    * @see {@link #backupFailurePolicy(BackupFailurePolicy backupFailurePolicy)}
    */
   public BackupFailurePolicy backupFailurePolicy() {
      return this.backupFailurePolicy;
   }

   /**
    * Configures whether the replication happens in a 1PC or 2PC for sync backups.
    * The default value is "false"
    */
   public BackupConfigurationBuilder useTwoPhaseCommit(boolean useTwoPhaseCommit) {
      this.useTwoPhaseCommit = useTwoPhaseCommit;
      return this;
   }

   /**
    * Configures whether this site is used for backing up data or not (defaults to true).
    */
   public BackupConfigurationBuilder enabled(boolean isEnabled) {
      this.enabled = isEnabled;
      return this;
   }

   public XSiteStateTransferConfigurationBuilder stateTransfer() {
      return this.stateTransferBuilder;
   }

   @Override
   public void validate() {
      takeOfflineBuilder.validate();
      stateTransferBuilder.validate();
      if (site == null)
         throw new CacheConfigurationException("The 'site' must be specified!");
      if (backupFailurePolicy == BackupFailurePolicy.CUSTOM && (failurePolicyClass == null)) {
         throw new CacheConfigurationException("It is required to specify a 'failurePolicyClass' when using a " +
                                                "custom backup failure policy!");
      }
   }

   @Override
   public BackupConfiguration create() {
      return new BackupConfiguration(site, strategy, replicationTimeout, backupFailurePolicy, failurePolicyClass,
                                     useTwoPhaseCommit, takeOfflineBuilder.create(), stateTransferBuilder.create(), enabled);
   }

   @Override
   public Builder read(BackupConfiguration template) {
      this.takeOfflineBuilder.read(template.takeOffline());
      this.stateTransferBuilder.read(template.stateTransfer());
      this.site = template.site();
      this.strategy = template.strategy();
      this.backupFailurePolicy = template.backupFailurePolicy();
      this.replicationTimeout = template.replicationTimeout();
      this.failurePolicyClass = template.failurePolicyClass();
      this.useTwoPhaseCommit = template.isTwoPhaseCommit();
      this.enabled = template.enabled();
      return this;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof BackupConfigurationBuilder)) return false;

      BackupConfigurationBuilder that = (BackupConfigurationBuilder) o;

      if (replicationTimeout != that.replicationTimeout) return false;
      if (backupFailurePolicy != that.backupFailurePolicy) return false;
      if (failurePolicyClass != null ? !failurePolicyClass.equals(that.failurePolicyClass) : that.failurePolicyClass != null)
         return false;
      if (site != null ? !site.equals(that.site) : that.site != null) return false;
      if (strategy != that.strategy) return false;
      if (takeOfflineBuilder != null ? !takeOfflineBuilder.equals(that.takeOfflineBuilder) : that.takeOfflineBuilder != null)
         return false;
      if (useTwoPhaseCommit != that.useTwoPhaseCommit) return false;
      if (enabled != that.enabled) return false;
      if (stateTransferBuilder != null ?
            !stateTransferBuilder.equals(that.stateTransferBuilder) :
            that.stateTransferBuilder != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = site != null ? site.hashCode() : 0;
      result = 31 * result + (strategy != null ? strategy.hashCode() : 0);
      result = 31 * result + (int) (replicationTimeout ^ (replicationTimeout >>> 32));
      result = 31 * result + (backupFailurePolicy != null ? backupFailurePolicy.hashCode() : 0);
      result = 31 * result + (failurePolicyClass != null ? failurePolicyClass.hashCode() : 0);
      result = 31 * result + (takeOfflineBuilder != null ? takeOfflineBuilder.hashCode() : 0);
      result = 31 * result + (stateTransferBuilder != null ? stateTransferBuilder.hashCode() : 0);
      result = 31 * result + (useTwoPhaseCommit ? 1 : 0);
      return result;
   }

   @Override
   public String toString() {
      return "BackupConfigurationBuilder{" +
            "site='" + site + '\'' +
            ", strategy=" + strategy +
            ", replicationTimeout=" + replicationTimeout +
            ", useTwoPhaseCommit=" + useTwoPhaseCommit +
            ", backupFailurePolicy=" + backupFailurePolicy +
            ", failurePolicyClass='" + failurePolicyClass + '\'' +
            ", takeOfflineBuilder=" + takeOfflineBuilder +
            ", stateTransferBuilder=" + stateTransferBuilder +
            ", enabled=" + enabled +
            '}';
   }
}
