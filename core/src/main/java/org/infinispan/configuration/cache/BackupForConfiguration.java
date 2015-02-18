package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Defines the remote caches for which this cache acts as a backup.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class BackupForConfiguration {
   public static final AttributeDefinition<String> REMOTE_CACHE = AttributeDefinition.<String>builder("remoteCache", null, String.class).immutable().build();
   public static final AttributeDefinition<String> REMOTE_SITE = AttributeDefinition.<String>builder("remoteSite", null, String.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(BackupForConfiguration.class, REMOTE_CACHE, REMOTE_SITE);
   }

   private final Attribute<String> remoteCache;
   private final Attribute<String> remoteSite;
   private final AttributeSet attributes;

   public BackupForConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.remoteCache = attributes.attribute(REMOTE_CACHE);
      this.remoteSite = attributes.attribute(REMOTE_SITE);
   }

   /**
    * @return the name of the remote site that backups data into this cache.
    */
   public String remoteCache() {
      return remoteCache.get();
   }

   /**
    * @return the name of the remote cache that backups data into this cache.
    */
   public String remoteSite() {
      return remoteSite.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      BackupForConfiguration other = (BackupForConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

   public boolean isBackupFor(String remoteSite, String remoteCache) {
      boolean remoteSiteMatches = remoteSite() != null && remoteSite().equals(remoteSite);
      boolean remoteCacheMatches = remoteCache() != null && this.remoteCache().equals(remoteCache);
      return remoteSiteMatches && remoteCacheMatches;
   }

   @Override
   public String toString() {
      return "BackupForConfiguration [attributes=" + attributes + "]";
   }

}
