package org.infinispan.globalstate;

import java.util.EnumSet;
import java.util.Map;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * The {@link LocalConfigurationManager} is responsible for applying on each node the configuration changes initiated
 * through the {@link org.infinispan.globalstate.GlobalConfigurationManager}.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public interface LocalConfigurationManager {
   /**
    * Initialization entry point for the {@link LocalConfigurationManager}
    * @param embeddedCacheManager
    * @param globalConfigurationManager
    */
   void initialize(EmbeddedCacheManager embeddedCacheManager, GlobalConfigurationManager globalConfigurationManager);
   /**
    * Checks whether this {@link LocalConfigurationManager} supports the supplied flags.
    * A {@link org.infinispan.commons.CacheConfigurationException} will be thrown in case this cannot be done.
    *
    */
   void validateFlags(EnumSet<CacheContainerAdmin.AdminFlag> flags);

   /**
    * Creates the cache using the supplied configuration and flags
    *
    * @param name the name of the cache to create
    * @param configuration the {@link Configuration} to use
    * @param flags the desired {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag}s
    */
   void createCache(String name, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags);

   /**
    * Removes the specified cache.
    *
    * @param name the name of the cache to remove
    */
   void removeCache(String name);

   /**
    * Loads all persisted cache configurations
    */
   Map<String, Configuration> loadAll();
}
