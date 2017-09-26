package org.infinispan.globalstate;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * The {@link GlobalConfigurationManager} is the main interface for sharing configuration across a cluster. It uses
 * an internal cache '___config' which holds the clustered cache configurations serialized in their XML form.
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
@Scope(Scopes.GLOBAL)
public interface GlobalConfigurationManager {
   String CONFIG_STATE_CACHE_NAME = "___config";
   /**
    * Defines a cluster-wide cache configuration
    *
    * @param cacheName the name of the configuration
    * @param configuration the configuration object
    */
   Configuration createCache(String cacheName, Configuration configuration);

   /**
    * Removes a cluster-wide cache and its configuration
    * @param cacheName
    */
   void removeCache(String cacheName);
}
