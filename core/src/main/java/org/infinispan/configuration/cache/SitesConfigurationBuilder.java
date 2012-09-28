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

import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.Builder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class SitesConfigurationBuilder extends AbstractConfigurationChildBuilder<SitesConfiguration> {

   private static final int DEFAULT_BACKUP_COUNT = 2;

   private final List<BackupConfigurationBuilder> backups = new ArrayList<BackupConfigurationBuilder>(DEFAULT_BACKUP_COUNT);

   private Set<String> inUseBackupSites = new HashSet<String>(DEFAULT_BACKUP_COUNT);

   private final BackupForBuilder backupForBuilder;

   private boolean disableBackups;

   public SitesConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      backupForBuilder = new BackupForBuilder(builder);
   }

   public BackupConfigurationBuilder addBackup() {
      BackupConfigurationBuilder bcb = new BackupConfigurationBuilder(getBuilder());
      backups.add(bcb);
      return bcb;
   }

   public List<BackupConfigurationBuilder> backups() {
      return backups;
   }

   public BackupForBuilder backupFor() {
      return backupForBuilder;
   }

   @Override
   public void validate() {
      backupForBuilder.validate();

      //don't allow two backups with the same name
      Set<String> backupNames = new HashSet<String>(DEFAULT_BACKUP_COUNT);

      for (BackupConfigurationBuilder bcb : backups) {
         if (!backupNames.add(bcb.site())) {
            throw new ConfigurationException("Multiple sites with name '" + bcb.site() + "' are configured. That is not allowed!");
         }
      }

      for (String site : inUseBackupSites) {
         boolean found = false;
         for (BackupConfigurationBuilder bcb : backups) {
            if (bcb.site().equals(site)) found = true;
         }
         if (!found) {
            throw new ConfigurationException("The site '" + site + "' should be defined within the set of backups!");
         }
      }
   }

   @Override
   public SitesConfiguration create() {
      List<BackupConfiguration> backupConfigurations = new ArrayList<BackupConfiguration>(DEFAULT_BACKUP_COUNT);
      for (BackupConfigurationBuilder bcb : this.backups) {
         backupConfigurations.add(bcb.create());
      }
      return new SitesConfiguration(backupConfigurations, backupForBuilder.create(), disableBackups, inUseBackupSites);
   }

   @Override
   public Builder read(SitesConfiguration template) {
      backupForBuilder.read(template.backupFor());
      for (BackupConfiguration bc : template.allBackups()) {
         BackupConfigurationBuilder bcb = new BackupConfigurationBuilder(getBuilder());
         bcb.read(bc);
         backups.add(bcb);
      }
      this.disableBackups = template.disableBackups();
      this.inUseBackupSites.addAll(template.inUseBackupSites());
      return this;
   }

   /**
    * Returns true if this cache won't backup its data remotely.
    * It would still accept other sites backing up data on this site.
    */
   public void disableBackups(boolean disable) {
      this.disableBackups = disable;
   }

   /**
    * Defines the site names, from the list of sites names defined within 'backups' element, to
    * which this cache backups its data.
    */
   public SitesConfigurationBuilder addInUseBackupSite(String site) {
      inUseBackupSites.add(site);
      return this;
   }

   /**
    * @see #backupSites(String)
    */
   public Set<String> inUseBackupSites() {
      return inUseBackupSites;
   }
}
