package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class BackupConfiguration extends ConfigurationElement<BackupConfiguration> {
   public static final AttributeDefinition<String> SITE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.SITE, null, String.class).autoPersist(false).immutable().build();
   public static final AttributeDefinition<BackupConfiguration.BackupStrategy> STRATEGY = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.STRATEGY, BackupConfiguration.BackupStrategy.ASYNC).immutable().build();
   public static final AttributeDefinition<Long> REPLICATION_TIMEOUT = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.TIMEOUT, 15000L).build();
   public static final AttributeDefinition<BackupFailurePolicy> FAILURE_POLICY = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.BACKUP_FAILURE_POLICY, BackupFailurePolicy.WARN).build();
   public static final AttributeDefinition<String> FAILURE_POLICY_CLASS = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.FAILURE_POLICY_CLASS, null, String.class).immutable().build();
   public static final AttributeDefinition<Boolean> USE_TWO_PHASE_COMMIT = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.USE_TWO_PHASE_COMMIT, false).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(BackupConfiguration.class, SITE, STRATEGY, REPLICATION_TIMEOUT, FAILURE_POLICY,  FAILURE_POLICY_CLASS, USE_TWO_PHASE_COMMIT);
   }

   private final TakeOfflineConfiguration takeOfflineConfiguration;
   private final XSiteStateTransferConfiguration xSiteStateTransferConfiguration ;

   public BackupConfiguration(AttributeSet attributes, TakeOfflineConfiguration takeOfflineConfiguration, XSiteStateTransferConfiguration xSiteStateTransferConfiguration) {
      super(Element.BACKUP, attributes, takeOfflineConfiguration, xSiteStateTransferConfiguration);
      this.takeOfflineConfiguration = takeOfflineConfiguration;
      this.xSiteStateTransferConfiguration = xSiteStateTransferConfiguration;
   }

   /**
    * Returns the name of the site where this cache backups its data.
    */
   public String site() {
      return attributes.attribute(SITE).get();
   }

   /**
    * How does the backup happen: sync or async.
    */
   public BackupStrategy strategy() {
      return attributes.attribute(STRATEGY).get();
   }

   public TakeOfflineConfiguration takeOffline() {
      return takeOfflineConfiguration;
   }

   /**
    * If the failure policy is set to {@link BackupFailurePolicy#CUSTOM} then the failurePolicyClass is required and
    * should return the fully qualified name of a class implementing {@link CustomFailurePolicy}
    */
   public String failurePolicyClass() {
      return attributes.attribute(FAILURE_POLICY_CLASS).get();
   }

   public boolean isAsyncBackup() {
      return strategy() == BackupStrategy.ASYNC;
   }

   public boolean isSyncBackup() {
      return strategy() == BackupStrategy.SYNC;
   }

   public long replicationTimeout() {
      return attributes.attribute(REPLICATION_TIMEOUT).get();
   }

   /**
    * @deprecated Since 14.0. To be removed without replacement
    */
   @Deprecated
   public BackupConfiguration replicationTimeout(long timeout) {
      return this;
   }

   public BackupFailurePolicy backupFailurePolicy() {
      return attributes.attribute(FAILURE_POLICY).get();
   }

   public enum BackupStrategy {
      SYNC, ASYNC
   }

   public boolean isTwoPhaseCommit() {
      return attributes.attribute(USE_TWO_PHASE_COMMIT).get();
   }

   /**
    * @see BackupConfigurationBuilder#enabled(boolean).
    * @deprecated Since 14.0. To be removed without replacement.
    */
   @Deprecated
   public boolean enabled() {
      return true;
   }

   public XSiteStateTransferConfiguration stateTransfer() {
      return xSiteStateTransferConfiguration;
   }
}
