/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.configuration.cache;

/**
 * Defines the remote caches for which this cache acts as a backup.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class BackupForConfiguration {
   private final String remoteCache;
   private final String remoteSite;

   public BackupForConfiguration(String remoteSite, String remoteCache) {
      this.remoteSite = remoteSite;
      this.remoteCache = remoteCache;
   }

   /**
    * @return the name of the remote site that backups data into this cache.
    */
   public String remoteCache() {
      return remoteCache;
   }

   /**
    * @return the name of the remote cache that backups data into this cache.
    */
   public String remoteSite() {
      return remoteSite;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof BackupForConfiguration)) return false;

      BackupForConfiguration that = (BackupForConfiguration) o;

      if (remoteCache != null ? !remoteCache.equals(that.remoteCache) : that.remoteCache != null) return false;
      if (remoteSite != null ? !remoteSite.equals(that.remoteSite) : that.remoteSite != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = remoteCache != null ? remoteCache.hashCode() : 0;
      result = 31 * result + (remoteSite != null ? remoteSite.hashCode() : 0);
      return result;
   }

   public boolean isBackupFor(String remoteSite, String remoteCache) {
      boolean remoteSiteMatches = this.remoteSite != null && this.remoteSite.equals(remoteSite);
      boolean remoteCacheMatches = this.remoteCache != null && this.remoteCache.equals(remoteCache);
      return remoteSiteMatches && remoteCacheMatches;
   }

   @Override
   public String toString() {
      return "BackupForConfiguration{" +
            "remoteCache='" + remoteCache + '\'' +
            ", remoteSite='" + remoteSite + '\'' +
            '}';
   }
}
