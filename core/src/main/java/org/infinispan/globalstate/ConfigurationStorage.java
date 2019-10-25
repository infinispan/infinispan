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
    * Prevents the creation or removal of caches.
    */
   IMMUTABLE,
   /**
    * Stores cache configurations in volatile storage. Only supports {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag#VOLATILE}
    */
   VOLATILE,
   /**
    * Persists cache configurations to the {@link GlobalStateConfiguration#persistentLocation()} in a <pre>caches.xml</pre> file that is read on startup.
    */
   OVERLAY,
   /**
    * Stores non-{@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag#VOLATILE} caches in a managed environment such as the server model. Supported in server deployments only.
    */
   MANAGED,
   /**
    * Lets you provide a configuration storage provider. Providers must implement the {@link LocalConfigurationStorage} interface.
    */
   CUSTOM,
}
