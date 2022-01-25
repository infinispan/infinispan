package org.infinispan.configuration.cache;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commons.configuration.attributes.AttributeCopier;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.xsite.spi.XSiteEntryMergePolicy;
import org.infinispan.xsite.spi.XSiteMergePolicy;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class SitesConfiguration extends ConfigurationElement<SitesConfiguration> {
   @SuppressWarnings("rawtypes")
   public static final AttributeDefinition<XSiteEntryMergePolicy> MERGE_POLICY = AttributeDefinition
         .builder(org.infinispan.configuration.parsing.Attribute.MERGE_POLICY, XSiteMergePolicy.DEFAULT, XSiteEntryMergePolicy.class)
         .copier(new MergePolicyAttributeCopier())
         .immutable()
         .build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SitesConfiguration.class, MERGE_POLICY);
   }

   private final BackupForConfiguration backupFor;
   private final List<BackupConfiguration> allBackups;

   public SitesConfiguration(AttributeSet attributes, List<BackupConfiguration> allBackups, BackupForConfiguration backupFor) {
      super(Element.SITES, attributes, ConfigurationElement.list(Element.BACKUPS, allBackups), backupFor);
      this.allBackups = Collections.unmodifiableList(allBackups);
      this.backupFor = backupFor;
   }

   /**
    * Returns true if this cache won't backup its data remotely. It would still accept other sites backing up data on
    * this site.
    *
    * @deprecated since 14.0. To be removed without replacement.
    */
   @Deprecated
   public boolean disableBackups() {
      return false;
   }

   /**
    * Returns the list of all sites where this cache might back up its data. The list of actual sites is defined by
    * {@link #inUseBackupSites}.
    */
   public List<BackupConfiguration> allBackups() {
      return allBackups;
   }

   public Stream<BackupConfiguration> allBackupsStream() {
      return allBackups.stream();
   }

   /**
    * Returns the list of {@link BackupConfiguration} that have {@link org.infinispan.configuration.cache.BackupConfiguration#enabled()} == true.
    * @deprecated Since 14.0. To be removed without replacement. Use {@link #allBackups()} or {@link #allBackupsStream()}.
    */
   @Deprecated
   public List<BackupConfiguration> enabledBackups() {
      return allBackups();
   }

   /**
    * @deprecated Since 14.0. To be removed without replacement. Use {@link #allBackups()} or {@link #allBackupsStream()}.
    */
   @Deprecated
   public Stream<BackupConfiguration> enabledBackupStream() {
      return allBackupsStream();
   }

   /**
    * @return information about caches that backup data into this cache.
    */
   public BackupForConfiguration backupFor() {
      return backupFor;
   }

   public BackupFailurePolicy getFailurePolicy(String siteName) {
      for (BackupConfiguration bc : allBackups) {
         if (bc.site().equals(siteName)) {
            return bc.backupFailurePolicy();
         }
      }
      throw new IllegalStateException("There must be a site configured for " + siteName);
   }

   /**
    * @deprecated since 14.0. To be removed without replacement
    */
   @Deprecated
   public boolean hasInUseBackup(String siteName) {
      return allBackups.stream().anyMatch(bc -> bc.site().equals(siteName));
   }

   /**
    * @deprecated since 14.0. To be removed without replacement. Use {@link #hasBackups()} instead.
    */
   @Deprecated
   public boolean hasEnabledBackups() {
      return hasBackups();
   }

   public boolean hasBackups() {
      return !allBackups.isEmpty();
   }

   public boolean hasSyncEnabledBackups() {
      return allBackupsStream().anyMatch(BackupConfiguration::isSyncBackup);
   }

   public Stream<BackupConfiguration> syncBackupsStream() {
      return allBackupsStream().filter(BackupConfiguration::isSyncBackup);
   }

   public boolean hasAsyncEnabledBackups() {
      return allBackupsStream().anyMatch(BackupConfiguration::isAsyncBackup);
   }

   public Stream<BackupConfiguration> asyncBackupsStream() {
      return allBackupsStream().filter(BackupConfiguration::isAsyncBackup);
   }

   /**
    * @deprecated since 14.0. To be removed without replacement.
    */
   @Deprecated
   public Set<String> inUseBackupSites() {
      return allBackups.stream().map(BackupConfiguration::site).collect(Collectors.toSet());
   }

   /**
    * @return The {@link XSiteEntryMergePolicy} to resolve conflicts when asynchronous cross-site replication is
    * enabled.
    * @see SitesConfigurationBuilder#mergePolicy(XSiteEntryMergePolicy)
    */
   public XSiteEntryMergePolicy<?, ?> mergePolicy() {
      return attributes.attribute(MERGE_POLICY).get();
   }

   @SuppressWarnings("rawtypes")
   private static class MergePolicyAttributeCopier implements AttributeCopier<XSiteEntryMergePolicy> {

      @Override
      public XSiteEntryMergePolicy copyAttribute(XSiteEntryMergePolicy attribute) {
         if (attribute == null) {
            return null;
         }
         if (attribute instanceof XSiteMergePolicy) {
            //the default implementations are immutable and can be reused.
            return ((XSiteMergePolicy) attribute).getInstance();
         } else {
            return Util.getInstance(attribute.getClass());
         }
      }
   }
}
