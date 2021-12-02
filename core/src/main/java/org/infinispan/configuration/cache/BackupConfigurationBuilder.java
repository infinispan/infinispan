package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.BackupConfiguration.ENABLED;
import static org.infinispan.configuration.cache.BackupConfiguration.FAILURE_POLICY;
import static org.infinispan.configuration.cache.BackupConfiguration.FAILURE_POLICY_CLASS;
import static org.infinispan.configuration.cache.BackupConfiguration.REPLICATION_TIMEOUT;
import static org.infinispan.configuration.cache.BackupConfiguration.SITE;
import static org.infinispan.configuration.cache.BackupConfiguration.STRATEGY;
import static org.infinispan.configuration.cache.BackupConfiguration.USE_TWO_PHASE_COMMIT;
import static org.infinispan.util.logging.Log.CONFIG;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class BackupConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<BackupConfiguration> {
   private final AttributeSet attributes;
   private final XSiteStateTransferConfigurationBuilder stateTransferBuilder;
   private final TakeOfflineConfigurationBuilder takeOfflineBuilder;

   public BackupConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = BackupConfiguration.attributeDefinitionSet();
      takeOfflineBuilder = new TakeOfflineConfigurationBuilder(builder, this);
      stateTransferBuilder = new XSiteStateTransferConfigurationBuilder(builder, this);
   }

   /**
    * @param site Specifies the name of the backup location for this cache. The name must match a site defined in the
    *             global configuration.
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
    * should return the fully qualified name of a class implementing {@link CustomFailurePolicy}
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
    * Sets the strategy used for backing up data: sync or async. Defaults to {@link
    * org.infinispan.configuration.cache.BackupConfiguration.BackupStrategy#ASYNC}.
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
    * Configures the policy for handling failed requests. Defaults to {@link BackupFailurePolicy#WARN}
    */
   public BackupConfigurationBuilder backupFailurePolicy(BackupFailurePolicy backupFailurePolicy) {
      attributes.attribute(FAILURE_POLICY).set(backupFailurePolicy);
      return this;
   }

   /**
    * Controls if replication happens in a two phase commit (2PC) for synchronous backups. The default value is {@code
    * false}, which means replication happens in a one phase commit (1PC).
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
      String siteName = attributes.attribute(SITE).get();
      if (siteName == null)
         throw CONFIG.backupMissingSite();

      BackupFailurePolicy policy = attributes.attribute(FAILURE_POLICY).get();
      boolean asyncStrategy = strategy() == BackupConfiguration.BackupStrategy.ASYNC;
      boolean hasFailurePolicyClass = failurePolicyClass() != null;
      switch (policy) {
         case CUSTOM:
            if (!hasFailurePolicyClass) {
               throw CONFIG.missingBackupFailurePolicyClass(siteName);
            }
            if (asyncStrategy) {
               throw CONFIG.invalidPolicyWithAsyncStrategy(siteName, policy);
            }
            break;
         case WARN:
         case IGNORE:
            if (hasFailurePolicyClass) {
               throw CONFIG.failurePolicyClassNotCompatibleWith(siteName, policy);
            }
            break;
         case FAIL:
            if (hasFailurePolicyClass) {
               throw CONFIG.failurePolicyClassNotCompatibleWith(siteName, policy);
            }
            if (asyncStrategy) {
               throw CONFIG.invalidPolicyWithAsyncStrategy(siteName, policy);
            }
            break;
         default:
            throw new IllegalStateException("Unexpected backup policy " + policy + " for backup " + siteName);
      }

      if (attributes.attribute(USE_TWO_PHASE_COMMIT).get() && asyncStrategy) {
         throw CONFIG.twoPhaseCommitAsyncBackup();
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
