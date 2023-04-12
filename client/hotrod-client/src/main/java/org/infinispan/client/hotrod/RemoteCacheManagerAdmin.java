package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.configuration.BasicConfiguration;

/**
 * Remote Administration operations
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public interface RemoteCacheManagerAdmin extends CacheContainerAdmin<RemoteCacheManagerAdmin, BasicConfiguration> {

   /**
    * Creates a cache on the remote server cluster using the specified template name.
    *
    * @param name the name of the cache to create
    * @param template the template to use for the cache. If null, the configuration marked as default on the server
    *                 will be used
    * @return the cache
    * @throws HotRodClientException
    */
   @Override
   <K, V> RemoteCache<K, V> createCache(String name, String template) throws HotRodClientException;

   /**
    * Creates a cache on the remote server cluster using the specified default configuration template
    * present in the server.
    *
    * @param name the name of the cache to create
    * @param template {@link DefaultTemplate} enum
    * @return the cache
    * @throws HotRodClientException
    */
   <K, V> RemoteCache<K, V> createCache(String name, DefaultTemplate template) throws HotRodClientException;

   /**
    * Creates a cache on the remote server cluster using the specified configuration
    *
    * @param name the name of the cache to create
    * @param configuration a concrete cache configuration that will be sent to the server in one of the supported formats:
    *                      XML, JSON, and YAML. The server detects the format automatically. The configuration must conform
    *                      to the Infinispan embedded configuration schema version that is supported by the server.
    *
    * @return the cache
    * @throws HotRodClientException
    */
   @Override
   <K, V> RemoteCache<K, V> createCache(String name, BasicConfiguration configuration) throws HotRodClientException;

   /**
    * Retrieves an existing cache on the remote server cluster. If it doesn't exist, it will be created using the
    * specified template name.
    *
    * @param name the name of the cache to create
    * @param template the template to use for the cache. If null, the configuration marked as default on the server
    *                 will be used
    * @return the cache
    * @throws HotRodClientException
    */
   @Override
   <K, V> RemoteCache<K, V> getOrCreateCache(String name, String template) throws HotRodClientException;

   /**
    *  Retrieves an existing cache on the remote server cluster. If it doesn't exist, it will be created using the
    *  specified default template that is present in the server.
    *
    * @param name the name of the cache to create
    * @param template {@link DefaultTemplate} enum
    * @return the cache
    * @throws HotRodClientException
    */
   <K, V> RemoteCache<K, V> getOrCreateCache(String name, DefaultTemplate template) throws HotRodClientException;

   /**
    * Retrieves an existing cache on the remote server cluster. If it doesn't exist, it will be created using the
    * specified configuration.
    *
    * @param name the name of the cache to create
    * @param configuration a concrete cache configuration of that will be sent to the server in one of the supported formats:
    *                      XML, JSON and YAML. The format will be detected automatically. The configuration must use the
    *                      Infinispan embedded configuration schema in a version supported by the server.
    * @return the cache
    * @throws HotRodClientException
    */
   @Override
   <K, V> RemoteCache<K, V> getOrCreateCache(String name, BasicConfiguration configuration) throws HotRodClientException;

   /**
    * Removes a cache from the remote server cluster.
    *
    * @param name the name of the cache to remove
    * @throws HotRodClientException
    */
   @Override
   void removeCache(String name) throws HotRodClientException;

   /**
    * Performs a mass reindexing of the specified cache. The command will return immediately and the reindexing will
    * be performed asynchronously
    * @param name the name of the cache to reindex
    * @throws HotRodClientException
    */
   void reindexCache(String name) throws HotRodClientException;

   /**
    * Updates the index schema state for the given cache,
    * the cache engine is hot restarted so that index persisted or not persisted state will be preserved.
    *
    * @param cacheName the name of the cache on which the index schema will be updated
    * @throws HotRodClientException
    */
   void updateIndexSchema(String cacheName) throws HotRodClientException;

   /**
    * Updates a mutable configuration attribute for the given cache.
    *
    * @param cacheName the name of the cache on which the attribute will be updated
    * @param attribute the path of the attribute we want to change
    * @param value the new value to apply to the attribute
    * @throws HotRodClientException
    */
   void updateConfigurationAttribute(String cacheName, String attribute, String value) throws HotRodClientException;

}
