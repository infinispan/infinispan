package org.infinispan.manager;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

/**
 * Cache manager operations which affect the whole cluster. An instance of this can be retrieved from
 * {@link EmbeddedCacheManager#administration()}
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public interface EmbeddedCacheManagerAdmin extends CacheContainerAdmin<EmbeddedCacheManagerAdmin, Configuration> {

   /**
    * Creates a cache on the container using the specified template.
    *
    * @param name     the name of the cache to create
    * @param template the template to use for the cache. If null, the configuration marked as default on the container
    *                 will be used
    * @return the cache
    *
    * @throws org.infinispan.commons.CacheException if a cache with the same name already exists
    */
   <K, V> Cache<K, V> createCache(String name, String template);

   /**
    * Retrieves an existing cache or creates one using the specified template if it doesn't exist
    *
    * @param name     the name of the cache to create
    * @param template the template to use for the cache. If null, the configuration marked as default on the container
    *                 will be used
    * @return the cache
    */
   <K, V> Cache<K, V> getOrCreateCache(String name, String template);

   /**
    * Creates a cache across the cluster. The cache will survive topology changes, e.g. when a new node joins the cluster,
    * it will automatically be created there. This method will wait for the cache to be created on all nodes before
    * returning.
    *
    * @param name the name of the cache
    * @param configuration the configuration to use. It must be a clustered configuration (e.g. distributed)
    * @param <K> the generic type of the key
    * @param <V> the generic type of the value
    * @return the cache
    *
    * @throws org.infinispan.commons.CacheException if a cache with the same name already exists
    */
   <K, V> Cache<K, V> createCache(String name, Configuration configuration);

   /**
    * Retrieves an existing cache or creates one across the cluster using the specified configuration.
    * The cache will survive topology changes, e.g. when a new node joins the cluster,
    * it will automatically be created there. This method will wait for the cache to be created on all nodes before
    * returning.
    *
    * @param name the name of the cache
    * @param configuration the configuration to use. It must be a clustered configuration (e.g. distributed)
    * @param <K> the generic type of the key
    * @param <V> the generic type of the value
    * @return the cache
    */
   <K, V> Cache<K, V> getOrCreateCache(String name, Configuration configuration);

   /**
    * Creates a template that is replicated across the cluster using the specified configuration.
    * The template will survive topology changes, e.g. when a new node joins the cluster,
    * it will automatically be created there. This method will wait for the template to be created on all nodes before
    * returning.
    *
    * @param name the name of the template
    * @param configuration the configuration to use. It must be a clustered configuration (e.g. distributed)
    * @throws org.infinispan.commons.CacheConfigurationException if a template with the same name already exists
    */
   void createTemplate(String name, Configuration configuration);

   /**
    * Retrieves an existing template or creates one across the cluster using the specified configuration.
    * The template will survive topology changes, e.g. when a new node joins the cluster,
    * it will automatically be created there. This method will wait for the template to be created on all nodes before
    * returning.
    *
    * @param name the name of the template
    * @param configuration the configuration to use. It must be a clustered configuration (e.g. distributed)
    * @return the template configuration
    */
   Configuration getOrCreateTemplate(String name, Configuration configuration);

   /**
    * Removes a template from the cache container. Any persisted data will be cleared.
    *
    * @param name the name of the template to remove
    */
   void removeTemplate(String name);

   /**
    * Performs any cache manager operations using the specified {@link Subject}. Only applies to cache managers with authorization
    * enabled (see {@link GlobalConfigurationBuilder#security()}.
    *
    * @param subject
    * @return an {@link EmbeddedCacheManagerAdmin} instance on which a real operation is to be invoked, using the specified subject
    */
   EmbeddedCacheManagerAdmin withSubject(Subject subject);
}
