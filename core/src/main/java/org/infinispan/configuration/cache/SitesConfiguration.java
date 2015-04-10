package org.infinispan.configuration.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeInitializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class SitesConfiguration {
   public static final AttributeDefinition<Boolean> DISABLE_BACKUPS = AttributeDefinition.builder("disableBackups", false).immutable().build();
   public static final AttributeDefinition<Set<String>> IN_USE_BACKUP_SITES = AttributeDefinition.builder("inUseBackupSites", null, (Class<Set<String>>)(Class<?>)Set.class)
         .initializer(new AttributeInitializer<Set<String>>() {
            @Override
            public Set<String> initialize() {
               return new HashSet<>(2);
            }
         }).immutable().build();

   static final AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SitesConfiguration.class, DISABLE_BACKUPS, IN_USE_BACKUP_SITES);
   }

   private final BackupForConfiguration backupFor;
   private final List<BackupConfiguration> allBackups;
   private final Attribute<Boolean> disableBackups;
   private final Attribute<Set<String>> inUseBackupSites;
   private final AttributeSet attributes;

   public SitesConfiguration(AttributeSet attributes, List<BackupConfiguration> allBackups, BackupForConfiguration backupFor) {
      this.attributes = attributes.checkProtection();
      this.allBackups = Collections.unmodifiableList(allBackups);
      this.disableBackups = attributes.attribute(DISABLE_BACKUPS);
      this.inUseBackupSites = attributes.attribute(IN_USE_BACKUP_SITES);
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
      List<BackupConfiguration> result = new ArrayList<>();
      for (BackupConfiguration bc : allBackups) {
         if (bc.enabled()) result.add(bc);
      }
      return result;
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
      for (BackupConfiguration bc : allBackups) {
         if (bc.enabled()) return true;
      }
      return false;
   }

   public Set<String> inUseBackupSites() {
      return inUseBackupSites.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      result = prime * result + ((backupFor == null) ? 0 : backupFor.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      SitesConfiguration other = (SitesConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      if (backupFor == null) {
         if (other.backupFor != null)
            return false;
      } else if (!backupFor.equals(other.backupFor))
         return false;
      return true;
   }

   @Override
   public String toString() {
      return "SitesConfiguration [backupFor=" + backupFor + ", allBackups=" + allBackups + ", attributes=" + attributes + "]";
   }
}
