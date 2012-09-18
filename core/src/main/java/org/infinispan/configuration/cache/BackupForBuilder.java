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

import org.infinispan.api.BasicCacheContainer;
import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.Builder;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class BackupForBuilder extends AbstractConfigurationChildBuilder<BackupForConfiguration> {
   private String remoteCache;
   private String remoteSite;

   public BackupForBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * @param siteName the name of the remote site that backups data into this cache.
    */
   public BackupForBuilder remoteSite(String siteName) {
      this.remoteSite = siteName;
      return this;
   }

   /**
    * @param name the name of the remote cache that backups data into this cache.
    */
   public BackupForBuilder remoteCache(String name) {
      if (name == null) {
         throw new RuntimeException("Null name not allowed (use 'defaultRemoteCache()' " +
                                          "in case you want to specify the default cache name).");
      }
      this.remoteCache = name;
      return this;
   }

   /**
    * Use this method if the remote cache that backups in this cache is the default cache.
    */
   public BackupForBuilder defaultRemoteCache() {
      this.remoteCache = BasicCacheContainer.DEFAULT_CACHE_NAME;
      return this;
   }

   @Override
   public void validate() {
      //if both remote cache and remote site are not specified then this is not a backup
      if (remoteCache == null && remoteSite == null)
         return;
      if (remoteSite == null || remoteCache == null) {
         throw new ConfigurationException("Both 'remoteCache' and 'remoteSite' must be specified for a backup'!");
      }
   }

   @Override
   public BackupForConfiguration create() {
      return new BackupForConfiguration(remoteSite, remoteCache);
   }

   @Override
   public Builder<?> read(BackupForConfiguration template) {
      this.remoteCache = template.remoteCache();
      this.remoteSite = template.remoteSite();
      return this;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof BackupForBuilder)) return false;

      BackupForBuilder that = (BackupForBuilder) o;

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

   @Override
   public String toString() {
      return "BackupForBuilder{" +
            "remoteCache='" + remoteCache + '\'' +
            ", remoteSite='" + remoteSite + '\'' +
            '}';
   }

   public void reset() {
      remoteCache = null;
      remoteSite = null;
   }
}
