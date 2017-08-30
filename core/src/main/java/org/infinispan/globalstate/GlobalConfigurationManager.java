package org.infinispan.globalstate;

import java.util.EnumSet;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * The {@link GlobalConfigurationManager} is the main interface for sharing runtime configuration state across a cluster.
 * It uses an internal cache '___config'. The cache is keyed with {@link ScopedState}. Each scope owner is responsible
 * for its own keys.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
@Scope(Scopes.GLOBAL)
public interface GlobalConfigurationManager {
   String CONFIG_STATE_CACHE_NAME = "___config";
   /**
    * Defines a cluster-wide cache configuration
    *  @param cacheName the name of the configuration
    * @param configuration the configuration object
    * @param flags
    */
   Configuration createCache(String cacheName, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags);

   /**
    * Removes a cluster-wide cache and its configuration
    * @param cacheName the name of the cache
    * @param flags
    */
   void removeCache(String cacheName, EnumSet<CacheContainerAdmin.AdminFlag> flags);

   /**
    * Returns the clustered configuration state cache
    * @return the clustered configuration state cache
    */
   Cache<ScopedState, Object> getStateCache();
}
