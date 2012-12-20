/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.configuration.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class SitesConfiguration {

   private final List<BackupConfiguration> allBackups;

   private final BackupForConfiguration backupFor;

   private final boolean disableBackups;

   private final Set<String> inUseBackupSites;

   public SitesConfiguration(List<BackupConfiguration> backups, BackupForConfiguration backupFor, boolean disableBackups,
                             Set<String> backupSites) {
      this.allBackups = Collections.unmodifiableList(new ArrayList<BackupConfiguration>(backups));
      this.backupFor = backupFor;
      this.disableBackups = disableBackups;
      this.inUseBackupSites = Collections.unmodifiableSet(new HashSet<String>(backupSites));
   }

   /**
    * Returns true if this cache won't backup its data remotely.
    * It would still accept other sites backing up data on this site.
    */
   public boolean disableBackups() {
      return disableBackups;
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
      List<BackupConfiguration> result = new ArrayList<BackupConfiguration>();
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


   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof SitesConfiguration)) return false;

      SitesConfiguration that = (SitesConfiguration) o;

      if (disableBackups != that.disableBackups) return false;
      if (backupFor != null ? !backupFor.equals(that.backupFor) : that.backupFor != null) return false;
      if (inUseBackupSites != null ? !inUseBackupSites.equals(that.inUseBackupSites) : that.inUseBackupSites != null) return false;
      if (allBackups != null ? !allBackups.equals(that.allBackups) : that.allBackups != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = allBackups != null ? allBackups.hashCode() : 0;
      result = 31 * result + (backupFor != null ? backupFor.hashCode() : 0);
      result = 31 * result + (disableBackups ? 1 : 0);
      result = 31 * result + (inUseBackupSites != null ? inUseBackupSites.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "SiteConfiguration{" +
            "allBackups=" + allBackups +
            ", backupFor=" + backupFor +
            ", disableBackups=" + disableBackups +
            ", inUseBackupSites=" + inUseBackupSites +
            '}';
   }
}
