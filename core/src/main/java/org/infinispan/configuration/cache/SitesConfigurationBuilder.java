package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.global.GlobalConfiguration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class SitesConfigurationBuilder extends AbstractConfigurationChildBuilder  implements Builder<SitesConfiguration> {

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
            throw new CacheConfigurationException("Multiple sites with name '" + bcb.site() + "' are configured. That is not allowed!");
         }
         bcb.validate();
      }

      for (String site : inUseBackupSites) {
         boolean found = false;
         for (BackupConfigurationBuilder bcb : backups) {
            if (bcb.site().equals(site)) found = true;
         }
         if (!found) {
            throw new CacheConfigurationException("The site '" + site + "' should be defined within the set of backups!");
         }
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      backupForBuilder.validate(globalConfig);

      for (BackupConfigurationBuilder bcb : backups) {
         bcb.validate(globalConfig);
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
}
