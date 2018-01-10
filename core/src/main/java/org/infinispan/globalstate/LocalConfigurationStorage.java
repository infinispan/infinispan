package org.infinispan.globalstate;

import java.util.EnumSet;
import java.util.Map;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * The {@link LocalConfigurationStorage} is responsible for applying on each node the configuration changes initiated
 * through the {@link org.infinispan.globalstate.GlobalConfigurationManager} and persist them if requested via
 * {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag#PERMANENT}.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public interface LocalConfigurationStorage {
   /**
    * Initialization entry point for the {@link LocalConfigurationStorage}
    * @param embeddedCacheManager
    */
   void initialize(EmbeddedCacheManager embeddedCacheManager);
   /**
    * Checks whether this {@link LocalConfigurationStorage} supports the supplied flags.
    * A {@link org.infinispan.commons.CacheConfigurationException} will be thrown in case this cannot be done.
    *
    */
   void validateFlags(EnumSet<CacheContainerAdmin.AdminFlag> flags);

   /**
    * Creates the cache using the supplied template, configuration and flags. This method may be invoked either with or
    * without a template. In both cases a concrete configuration will also be available. If a template name is present,
    * the {@link LocalConfigurationStorage} should use it, e.g. when persisting the configuration.
    *
    * @param name the name of the cache to create
    * @param template the template that should be used to configure the cache. Can be null.
    * @param configuration the {@link Configuration} to use
    * @param flags the desired {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag}s
    */
   void createCache(String name, String template, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags);

   /**
    * Removes the specified cache.
    *
    * @param name the name of the cache to remove
    * @param flags the desired {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag}s
    */
   void removeCache(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags);

   /**
    * Loads all persisted cache configurations
    */
   Map<String, Configuration> loadAll();
}
