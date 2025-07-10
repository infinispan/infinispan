package org.infinispan.globalstate;

import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * The {@link LocalConfigurationStorage} is responsible for applying on each node the configuration changes initiated
 * through the {@link org.infinispan.globalstate.GlobalConfigurationManager} and persist them unless they are
 * {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag#VOLATILE}.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public interface LocalConfigurationStorage {
   /**
    * Initialization entry point for the {@link LocalConfigurationStorage}
    * @param embeddedCacheManager
    * @param configurationManager
    * @param blockingManager handler to use when a blocking operation is required
    */
   void initialize(EmbeddedCacheManager embeddedCacheManager, ConfigurationManager configurationManager, BlockingManager blockingManager);
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
   CompletionStage<Void> createCache(String name, String template, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags);

   /**
    * Creates the template using the supplied configuration and flags.
    *
    * @param name the name of the template to create
    * @param configuration the {@link Configuration} to use
    * @param flags the desired {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag}s
    */
   CompletionStage<Void> createTemplate(String name, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags);

   /**
    * Updates an existing configuration. Only the attributes that are mutable and that have been modified in the supplied
    * configuration will be applied.
    *
    * @param name the name of the configuration (cache/template)
    * @param configuration the configuration changes to apply
    * @param flags the desired {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag}s
    */
   CompletionStage<Void> updateConfiguration(String name, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags);

   /**
    * Validates an update to an existing configuration.
    * @param name the name of the configuration (cache/template)
    * @param configuration the configuration changes to apply
    * @param flags the desired {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag}s
    */
   CompletionStage<Void> validateConfigurationUpdate(String name, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags);

   /**
    * Removes the specified cache.
    *
    * @param name the name of the cache to remove
    * @param flags the desired {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag}s
    */
   CompletionStage<Void> removeCache(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags);

   /**
    * Removes the specified template.
    *
    * @param name the name of the template to remove
    * @param flags the desired {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag}s
    */
   CompletionStage<Void> removeTemplate(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags);

   /**
    * Loads all persisted cache configurations
    */
   Map<String, Configuration> loadAllCaches();

   /**
    * Loads all persisted templates
    */
   Map<String, Configuration> loadAllTemplates();
}
