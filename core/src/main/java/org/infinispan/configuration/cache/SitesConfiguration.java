package org.infinispan.configuration.cache;

import static org.infinispan.commons.configuration.attributes.AttributeValidator.greaterThanZero;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commons.configuration.attributes.Attribute;
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
   public static final AttributeDefinition<Boolean> DISABLE_BACKUPS = AttributeDefinition.builder("disable", false).immutable().build();
   @SuppressWarnings("unchecked")
   public static final AttributeDefinition<Set<String>> IN_USE_BACKUP_SITES = AttributeDefinition.builder("backup-sites-in-use", null, (Class<Set<String>>) (Class<?>) Set.class).initializer(() -> new HashSet<>(2)).autoPersist(false).immutable().build();
   @SuppressWarnings("rawtypes")
   public static final AttributeDefinition<XSiteEntryMergePolicy> MERGE_POLICY = AttributeDefinition
         .builder(org.infinispan.configuration.parsing.Attribute.MERGE_POLICY, XSiteMergePolicy.DEFAULT, XSiteEntryMergePolicy.class)
         .copier(new MergePolicyAttributeCopier())
         .immutable()
         .build();
   public static final AttributeDefinition<Long> MAX_CLEANUP_DELAY = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.MAX_CLEANUP_DELAY, 30000L)
         .validator(greaterThanZero(org.infinispan.configuration.parsing.Attribute.MAX_CLEANUP_DELAY))
         .immutable()
         .build();
   public static final AttributeDefinition<Integer> TOMBSTONE_MAP_SIZE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.TOMBSTONE_MAP_SIZE, 512000)
         .validator(greaterThanZero(org.infinispan.configuration.parsing.Attribute.TOMBSTONE_MAP_SIZE))
         .immutable()
         .build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SitesConfiguration.class, DISABLE_BACKUPS, IN_USE_BACKUP_SITES, MERGE_POLICY, MAX_CLEANUP_DELAY, TOMBSTONE_MAP_SIZE);
   }

   private final BackupForConfiguration backupFor;
   private final List<BackupConfiguration> allBackups;
   private final Attribute<Boolean> disableBackups;
   @SuppressWarnings("rawtypes")
   private final Attribute<XSiteEntryMergePolicy> mergePolicy;
   private final Attribute<Set<String>> inUseBackupSites;

   public SitesConfiguration(AttributeSet attributes, List<BackupConfiguration> allBackups, BackupForConfiguration backupFor) {
      super(Element.SITES, attributes, ConfigurationElement.list(Element.BACKUPS, allBackups), backupFor);
      this.allBackups = Collections.unmodifiableList(allBackups);
      this.disableBackups = attributes.attribute(DISABLE_BACKUPS);
      this.inUseBackupSites = attributes.attribute(IN_USE_BACKUP_SITES);
      this.mergePolicy = attributes.attribute(MERGE_POLICY);
      this.backupFor = backupFor;
   }

   /**
    * Returns true if this cache won't backup its data remotely.
    * It would still accept other sites backing up data on this site.
    */
   public boolean disableBackups() {
      return disableBackups.get();
   }

   /**
    * Returns the list of all sites where this cache might back up its data. The list of actual sites is defined by
    * {@link #inUseBackupSites}.
    */
   public List<BackupConfiguration> allBackups() {
      return allBackups;
   }

   /**
    * Returns the list of {@link BackupConfiguration} that have {@link org.infinispan.configuration.cache.BackupConfiguration#enabled()} == true.
    */
   public List<BackupConfiguration> enabledBackups() {
      return enabledBackupStream().collect(Collectors.toList());
   }

   public Stream<BackupConfiguration> enabledBackupStream() {
      return allBackups.stream().filter(BackupConfiguration::enabled);
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

   public boolean hasInUseBackup(String siteName) {
      for (BackupConfiguration bc : allBackups) {
         if (bc.site().equals(siteName)) {
            return bc.enabled();
         }
      }
      return false;
   }

   public boolean hasEnabledBackups() {
      return allBackups.stream().anyMatch(BackupConfiguration::enabled);
   }

   public boolean hasSyncEnabledBackups() {
      return enabledBackupStream().anyMatch(BackupConfiguration::isSyncBackup);
   }

   public Stream<BackupConfiguration> syncBackupsStream() {
      return enabledBackupStream().filter(BackupConfiguration::isSyncBackup);
   }

   public boolean hasAsyncEnabledBackups() {
      return enabledBackupStream().anyMatch(BackupConfiguration::isAsyncBackup);
   }

   public Stream<BackupConfiguration> asyncBackupsStream() {
      return enabledBackupStream().filter(BackupConfiguration::isAsyncBackup);
   }

   public Set<String> inUseBackupSites() {
      return inUseBackupSites.get();
   }

   /**
    * @return The {@link XSiteEntryMergePolicy} to resolve conflicts when asynchronous cross-site replication is
    * enabled.
    * @see SitesConfigurationBuilder#mergePolicy(XSiteEntryMergePolicy)
    */
   public XSiteEntryMergePolicy<?, ?> mergePolicy() {
      return mergePolicy.get();
   }

   /**
    * @return The maximum delay, in milliseconds, between which tombstone cleanup tasks run.
    */
   public long maxTombstoneCleanupDelay() {
      return attributes.attribute(MAX_CLEANUP_DELAY).get();
   }

   /**
    * @return The target tombstone map size.
    */
   public int tombstoneMapSize() {
      return attributes.attribute(TOMBSTONE_MAP_SIZE).get();
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
