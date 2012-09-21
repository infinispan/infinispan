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

import java.util.List;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class SitesConfiguration {

   private final List<BackupConfiguration> backups;

   private final BackupForConfiguration backupFor;

   private final boolean disableBackups;

   public SitesConfiguration(List<BackupConfiguration> backups, BackupForConfiguration backupFor, boolean disableBackups) {
      this.backups = backups;
      this.backupFor = backupFor;
      this.disableBackups = disableBackups;
   }

   /**
    * Returns true if this cache won't backup its data remotely.
    * It would still accept other sites backing up data on this site.
    */
   public boolean disableBackups() {
      return disableBackups;
   }

   /**
    * Returns the list of sites where this cache backups its data.
    */
   public List<BackupConfiguration> backups() {
      return backups;
   }

   /**
    * @return information about caches that backup data into this cache.
    */
   public BackupForConfiguration backupFor() {
      return backupFor;
   }

   public BackupFailurePolicy getFailurePolicy(String siteName) {
      for (BackupConfiguration bc : backups) {
         if (bc.site().equals(siteName)) {
            return bc.backupFailurePolicy();
         }
      }
      throw new IllegalStateException("There must be a site configured for " + siteName);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof SitesConfiguration)) return false;

      SitesConfiguration that = (SitesConfiguration) o;

      if (disableBackups != that.disableBackups) return false;
      if (backupFor != null ? !backupFor.equals(that.backupFor) : that.backupFor != null) return false;
      if (backups != null ? !backups.equals(that.backups) : that.backups != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = backups != null ? backups.hashCode() : 0;
      result = 31 * result + (backupFor != null ? backupFor.hashCode() : 0);
      result = 31 * result + (disableBackups ? 1 : 0);
      return result;
   }

   @Override
   public String toString() {
      return "SitesConfiguration{" +
            "backups=" + backups +
            ", backupFor=" + backupFor +
            ", disableBackups=" + disableBackups +
            '}';
   }
}
