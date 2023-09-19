package org.infinispan.configuration.cache;

import java.util.Objects;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

/**
 * Defines the remote caches for which this cache acts as a backup.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class BackupForConfiguration extends ConfigurationElement<BackupForConfiguration> {
   public static final AttributeDefinition<String> REMOTE_CACHE = AttributeDefinition.<String>builder(org.infinispan.configuration.parsing.Attribute.REMOTE_CACHE, null, String.class).immutable().build();
   public static final AttributeDefinition<String> REMOTE_SITE = AttributeDefinition.<String>builder(org.infinispan.configuration.parsing.Attribute.REMOTE_SITE, null, String.class).immutable().build();
   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(BackupForConfiguration.class, REMOTE_CACHE, REMOTE_SITE);
   }

   public BackupForConfiguration(AttributeSet attributes) {
      super(Element.BACKUP_FOR, attributes);
   }

   /**
    * @return the name of the remote site that backups data into this cache.
    */
   public String remoteCache() {
      return attributes.attribute(REMOTE_CACHE).get();
   }

   /**
    * @return the name of the remote cache that backups data into this cache.
    */
   public String remoteSite() {
      return attributes.attribute(REMOTE_SITE).get();
   }

   public boolean isBackupFor(String remoteSite, String remoteCache) {
      return Objects.equals(remoteSite(), remoteSite) && Objects.equals(remoteCache(), remoteCache);
   }
}
