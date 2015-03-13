package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class BackupConfiguration {
   static final AttributeDefinition<String> SITE = AttributeDefinition.builder("site", null, String.class).immutable().build();
   static final AttributeDefinition<BackupConfiguration.BackupStrategy> STRATEGY = AttributeDefinition.builder("strategy", BackupConfiguration.BackupStrategy.ASYNC).immutable().build();
   static final AttributeDefinition<Long> REPLICATION_TIMEOUT = AttributeDefinition.builder("replicationTimeout", 10000l).build();
   static final AttributeDefinition<BackupFailurePolicy> FAILURE_POLICY = AttributeDefinition.builder("backupFailurePolicy", BackupFailurePolicy.WARN).build();
   static final AttributeDefinition<String> FAILURE_POLICY_CLASS = AttributeDefinition.builder("failurePolicyClass", null, String.class).immutable().build();
   static final AttributeDefinition<Boolean> USE_TWO_PHASE_COMMIT = AttributeDefinition.builder("useTwoPhaseCommit", false).immutable().build();
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", true).immutable().build();
   static AttributeSet attributeSet() {
      return new AttributeSet(BackupConfiguration.class, SITE, STRATEGY, REPLICATION_TIMEOUT, FAILURE_POLICY,  FAILURE_POLICY_CLASS, USE_TWO_PHASE_COMMIT, ENABLED);
   }

   private final AttributeSet attributes;
   private final TakeOfflineConfiguration takeOfflineConfiguration;
   private final XSiteStateTransferConfiguration xSiteStateTransferConfiguration ;

   public BackupConfiguration(AttributeSet attributes, TakeOfflineConfiguration takeOfflineConfiguration, XSiteStateTransferConfiguration xSiteStateTransferConfiguration) {
      attributes.checkProtection();
      this.attributes = attributes;
      this.takeOfflineConfiguration = takeOfflineConfiguration;
      this.xSiteStateTransferConfiguration = xSiteStateTransferConfiguration;
   }

   /**
    * Returns the name of the site where this cache backups its data.
    */
   public String site() {
      return attributes.attribute(SITE).asString();
   }

   /**
    * How does the backup happen: sync or async.
    */
   public BackupStrategy strategy() {
      return attributes.attribute(STRATEGY).asObject(BackupStrategy.class);
   }

   public TakeOfflineConfiguration takeOffline() {
      return takeOfflineConfiguration;
   }

   /**
    * If the failure policy is set to {@link BackupFailurePolicy#CUSTOM} then the failurePolicyClass is required and
    * should return the fully qualified name of a class implementing {@link org.infinispan.xsite.CustomFailurePolicy}
    */
   public String failurePolicyClass() {
      return attributes.attribute(FAILURE_POLICY_CLASS).asString();
   }

   public boolean isAsyncBackup() {
      return strategy() == BackupStrategy.ASYNC;
   }

   public long replicationTimeout() {
      return attributes.attribute(REPLICATION_TIMEOUT).asLong();
   }

   public BackupConfiguration replicationTimeout(long timeout) {
      attributes.attribute(REPLICATION_TIMEOUT).set(timeout);
      return this;
   }

   public BackupFailurePolicy backupFailurePolicy() {
      return attributes.attribute(FAILURE_POLICY).asObject(BackupFailurePolicy.class);
   }

   public enum BackupStrategy {
      SYNC, ASYNC
   }

   public boolean isTwoPhaseCommit() {
      return attributes.attribute(USE_TWO_PHASE_COMMIT).asBoolean();
   }

   /**
    * @see BackupConfigurationBuilder#enabled(boolean).
    */
   public boolean enabled() {
      return attributes.attribute(ENABLED).asBoolean();
   }

   public XSiteStateTransferConfiguration stateTransfer() {
      return xSiteStateTransferConfiguration;
   }

   AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "BackupConfiguration [attributes=" + attributes + ", takeOfflineConfiguration=" + takeOfflineConfiguration
            + ", xSiteStateTransferConfiguration=" + xSiteStateTransferConfiguration + "]";
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      BackupConfiguration other = (BackupConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      if (takeOfflineConfiguration == null) {
         if (other.takeOfflineConfiguration != null)
            return false;
      } else if (!takeOfflineConfiguration.equals(other.takeOfflineConfiguration))
         return false;
      if (xSiteStateTransferConfiguration == null) {
         if (other.xSiteStateTransferConfiguration != null)
            return false;
      } else if (!xSiteStateTransferConfiguration.equals(other.xSiteStateTransferConfiguration))
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      result = prime * result + ((takeOfflineConfiguration == null) ? 0 : takeOfflineConfiguration.hashCode());
      result = prime * result
            + ((xSiteStateTransferConfiguration == null) ? 0 : xSiteStateTransferConfiguration.hashCode());
      return result;
   }
}
