package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.BackupForConfiguration.REMOTE_CACHE;
import static org.infinispan.configuration.cache.BackupForConfiguration.REMOTE_SITE;

import java.lang.invoke.MethodHandles;

import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
/**
 * @author Mircea Markus
 * @since 5.2
 */
public class BackupForBuilder extends AbstractConfigurationChildBuilder implements Builder<BackupForConfiguration> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   private final AttributeSet attributes;

   public BackupForBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = BackupForConfiguration.attributeDefinitionSet();
   }

   /**
    * @param siteName the name of the remote site that backups data into this cache.
    */
   public BackupForBuilder remoteSite(String siteName) {
      attributes.attribute(REMOTE_SITE).set(siteName);
      return this;
   }

   /**
    * @param name the name of the remote cache that backups data into this cache.
    */
   public BackupForBuilder remoteCache(String name) {
      if (name == null) {
         throw log.backupForNullCache();
      }
      attributes.attribute(REMOTE_CACHE).set(name);
      return this;
   }

   /**
    * Use this method if the remote cache that backups in this cache is the default cache.
    */
   public BackupForBuilder defaultRemoteCache() {
      attributes.attribute(REMOTE_CACHE).set(BasicCacheContainer.DEFAULT_CACHE_NAME);
      return this;
   }

   @Override
   public void validate() {
      //if both remote cache and remote site are not specified then this is not a backup
      if (attributes.attribute(REMOTE_CACHE).get() == null && attributes.attribute(REMOTE_SITE).get() == null)
         return;
      if (attributes.attribute(REMOTE_SITE).get() == null || attributes.attribute(REMOTE_CACHE).get() == null) {
         throw log.backupForMissingParameters();
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public BackupForConfiguration create() {
      return new BackupForConfiguration(attributes.protect());
   }

   @Override
   public BackupForBuilder read(BackupForConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "BackupForBuilder [attributes=" + attributes + "]";
   }

   public void reset() {
      attributes.reset();
   }
}
