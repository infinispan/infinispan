package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.BackupConfiguration.*;

import java.lang.invoke.MethodHandles;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class BackupConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<BackupConfiguration> {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   private final AttributeSet attributes;
   private XSiteStateTransferConfigurationBuilder stateTransferBuilder;
   private TakeOfflineConfigurationBuilder takeOfflineBuilder;

   public BackupConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = BackupConfiguration.attributeDefinitionSet();
      takeOfflineBuilder = new TakeOfflineConfigurationBuilder(builder, this);
      stateTransferBuilder = new XSiteStateTransferConfigurationBuilder(builder, this);
   }

   /**
    * @param site The name of the site where this cache backups. Must be a valid name, i.e. a site defined in the
    *             global config.
    */
   public BackupConfigurationBuilder site(String site) {
      attributes.attribute(SITE).set(site);
      return this;
   }

   /**
    * @see #site(String)
    */
   public String site() {
      return attributes.attribute(SITE).get();
   }

   /**
    * If the failure policy is set to {@link BackupFailurePolicy#CUSTOM} then the failurePolicyClass is required and
    * should return the fully qualified name of a class implementing {@link org.infinispan.xsite.CustomFailurePolicy}
    */
   public String failurePolicyClass() {
      return attributes.attribute(FAILURE_POLICY_CLASS).get();
   }

   /**
    * @see #failurePolicyClass()
    */
   public BackupConfigurationBuilder failurePolicyClass(String failurePolicy) {
      attributes.attribute(FAILURE_POLICY_CLASS).set(failurePolicy);
      return this;
   }

   /**
    * Timeout(millis) used for replicating calls to other sites.
    */
   public BackupConfigurationBuilder replicationTimeout(long replicationTimeout) {
      attributes.attribute(REPLICATION_TIMEOUT).set(replicationTimeout);
      return this;
   }

   /**
    * Sets the strategy used for backing up data: sync or async. If not specified defaults
    * to {@link org.infinispan.configuration.cache.BackupConfiguration.BackupStrategy#ASYNC}.
    */
   public BackupConfigurationBuilder strategy(BackupConfiguration.BackupStrategy strategy) {
      attributes.attribute(STRATEGY).set(strategy);
      return this;
   }

   /**
    * @see #strategy()
    */
   public BackupConfiguration.BackupStrategy strategy() {
      return attributes.attribute(STRATEGY).get();
   }

   public TakeOfflineConfigurationBuilder takeOffline() {
      return takeOfflineBuilder;
   }

   /**
    * Configures how the system behaves when the backup call fails. Only applies to sync backups.
    * The default values is  {@link org.infinispan.configuration.cache.BackupFailurePolicy#WARN}
    */
   public BackupConfigurationBuilder backupFailurePolicy(BackupFailurePolicy backupFailurePolicy) {
      attributes.attribute(FAILURE_POLICY).set(backupFailurePolicy);
      return this;
   }

   /**
    * Configures whether the replication happens in a 1PC or 2PC for sync backups.
    * The default value is "false"
    */
   public BackupConfigurationBuilder useTwoPhaseCommit(boolean useTwoPhaseCommit) {
      attributes.attribute(USE_TWO_PHASE_COMMIT).set(useTwoPhaseCommit);
      return this;
   }

   /**
    * Configures whether this site is used for backing up data or not (defaults to true).
    */
   public BackupConfigurationBuilder enabled(boolean isEnabled) {
      attributes.attribute(ENABLED).set(isEnabled);
      return this;
   }

   public XSiteStateTransferConfigurationBuilder stateTransfer() {
      return this.stateTransferBuilder;
   }

   @Override
   public void validate() {
      takeOfflineBuilder.validate();
      stateTransferBuilder.validate();
      if (attributes.attribute(SITE).get() == null)
         throw log.backupMissingSite();
      if (attributes.attribute(FAILURE_POLICY).get() == BackupFailurePolicy.CUSTOM && (attributes.attribute(FAILURE_POLICY_CLASS).get() == null)) {
         throw log.missingBackupFailurePolicyClass();
      }
      if (attributes.attribute(USE_TWO_PHASE_COMMIT).get() && attributes.attribute(STRATEGY).get() == BackupConfiguration.BackupStrategy.ASYNC) {
         throw log.twoPhaseCommitAsyncBackup();
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      takeOfflineBuilder.validate(globalConfig);
      stateTransferBuilder.validate(globalConfig);
   }

   @Override
   public BackupConfiguration create() {
      return new BackupConfiguration(attributes.protect(), takeOfflineBuilder.create(), stateTransferBuilder.create());
   }

   @Override
   public BackupConfigurationBuilder read(BackupConfiguration template) {
      attributes.read(template.attributes());
      takeOfflineBuilder.read(template.takeOffline());
      stateTransferBuilder.read(template.stateTransfer());
      return this;
   }

   @Override
   public String toString() {
      return this.getClass().getSimpleName() + attributes;
   }
}
