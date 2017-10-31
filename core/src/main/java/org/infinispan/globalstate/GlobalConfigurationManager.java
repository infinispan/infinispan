package org.infinispan.globalstate;

import java.util.EnumSet;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * The {@link GlobalConfigurationManager} is the main interface for sharing runtime configuration state across a cluster.
 * It uses an internal cache 'org.infinispan.CONFIG'. The cache is keyed with {@link ScopedState}. Each scope owner is responsible
 * for its own keys.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
@Scope(Scopes.GLOBAL)
public interface GlobalConfigurationManager {
   String CONFIG_STATE_CACHE_NAME = "org.infinispan.CONFIG";
   /**
    * Defines a cluster-wide cache configuration
    * @param cacheName the name of the configuration
    * @param configuration the configuration object
    * @param flags the flags to apply
    */
   Configuration createCache(String cacheName, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags);

   /**
    * Defines a cluster-wide cache configuration using the supplied template
    * @param cacheName the name of the configuration
    * @param template the template name to use
    * @param flags the flags to apply
    */
   Configuration createCache(String cacheName, String template, EnumSet<CacheContainerAdmin.AdminFlag> flags);

   /**
    * Removes a cluster-wide cache and its configuration
    * @param cacheName the name of the cache
    * @param flags
    */
   void removeCache(String cacheName, EnumSet<CacheContainerAdmin.AdminFlag> flags);
}
