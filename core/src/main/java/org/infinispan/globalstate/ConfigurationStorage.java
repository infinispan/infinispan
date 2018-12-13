package org.infinispan.globalstate;

import org.infinispan.configuration.global.GlobalStateConfiguration;

/**
 * Configuration storage
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public enum ConfigurationStorage {
   /**
    * An immutable configuration storage provider. This forbids creating/removing caches.
    */
   IMMUTABLE,
   /**
    * A volatile configuration storage provider which doesn't support {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag#PERMANENT}.
    */
   VOLATILE,
   /**
    * A configuration storage provider which stores configurations on the filesystem.
    * Cache configurations are persisted to the {@link GlobalStateConfiguration#persistentLocation()} in a <pre>caches.xml</pre> file which is read on startup.
    */
   OVERLAY,
   /**
    * A configuration storage provider which stores configurations in a managed environment, for example the server model.
    */
   MANAGED,
   /**
    * A custom configuration storage provider. Providers must implement the {@link LocalConfigurationStorage} interface.
    */
   CUSTOM,
}
