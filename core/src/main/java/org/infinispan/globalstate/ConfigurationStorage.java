package org.infinispan.globalstate;

/**
 * Configuration storage
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public enum ConfigurationStorage {
   /**
    * A volatile configuration storage provider which doesn't support {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag#PERMANENT}
    */
   VOLATILE,
   /**
    * A configuration storage provider which stores configurations on the filesystem
    */
   OVERLAY,
   /**
    * A configuration storage provider which stores configurations in a managed environment (e.g. the server model)
    */
   MANAGED,
   /**
    * A custom configuration storage provider
    */
   CUSTOM;
}
