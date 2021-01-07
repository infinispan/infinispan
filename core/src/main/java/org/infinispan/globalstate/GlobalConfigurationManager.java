package org.infinispan.globalstate;

import java.util.EnumSet;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin.AdminFlag;
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
    * Returns the global state cache
    */
   Cache<ScopedState, Object> getStateCache();

   /**
    * Defines a cluster-wide configuration template
    *
    * @param name the name of the template
    * @param configuration the configuration object
    * @param flags the flags to apply
    */
   CompletionStage<Void> createTemplate(String name, Configuration configuration, EnumSet<AdminFlag> flags);

   /**
    * Defines a cluster-wide configuration template
    *
    * @param name the name of the template
    * @param configuration the configuration object
    * @param flags the flags to apply
    */
   CompletionStage<Configuration> getOrCreateTemplate(String name, Configuration configuration, EnumSet<AdminFlag> flags);

   /**
    * Defines a cluster-wide cache configuration
    * @param cacheName the name of the configuration
    * @param configuration the configuration object
    * @param flags the flags to apply
    */
   CompletionStage<Configuration> createCache(String cacheName, Configuration configuration, EnumSet<AdminFlag> flags);

   /**
    * Defines a cluster-wide cache configuration or retrieves an existing one
    * @param cacheName the name of the configuration
    * @param configuration the configuration object
    * @param flags the flags to apply
    */
   CompletionStage<Configuration> getOrCreateCache(String cacheName, Configuration configuration, EnumSet<AdminFlag> flags);

   /**
    * Defines a cluster-wide cache configuration using the supplied template
    * @param cacheName the name of the configuration
    * @param template the template name to use
    * @param flags the flags to apply
    */
   CompletionStage<Configuration> createCache(String cacheName, String template, EnumSet<AdminFlag> flags);

   /**
    * Defines a cluster-wide cache configuration using the supplied template or retrieves an existing one
    * @param cacheName the name of the configuration
    * @param template the template name to use
    * @param flags the flags to apply
    */
   CompletionStage<Configuration> getOrCreateCache(String cacheName, String template, EnumSet<AdminFlag> flags);

   /**
    * Removes a cluster-wide cache and its configuration
    * @param cacheName the name of the cache
    * @param flags
    */
   CompletionStage<Void> removeCache(String cacheName, EnumSet<AdminFlag> flags);

   /**
    * Removes a cluster-wide template
    * @param name the name of the template
    * @param flags
    */
   CompletionStage<Void> removeTemplate(String name, EnumSet<AdminFlag> flags);
}
