package org.infinispan.client.hotrod;

import java.util.EnumSet;

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
    * Creates a cache on the remote server cluster using the specified template.
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
    * Creates a cache on the remote server cluster using the specified configuration
    *
    * @param name the name of the cache to create
    * @param configuration a concrete configuration that will be sent to the server. The XML representation of this
    *                      configuration must use the Infinispan embedded configuration schema in a version supported
    *                      by the server.
    * @return the cache
    * @throws HotRodClientException
    */
   @Override
   <K, V> RemoteCache<K, V> createCache(String name, BasicConfiguration configuration) throws HotRodClientException;

   /**
    * Retrieves an existing cache on the remote server cluster. If it doesn't exist, it will be created using the
    * specified template.
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
    * Retrieves an existing cache on the remote server cluster. If it doesn't exist, it will be created using the
    * specified configuration.
    *
    * @param name the name of the cache to create
    * @param configuration a concrete configuration that will be sent to the server. The XML representation of this
    *                      configuration must use the Infinispan embedded configuration schema in a version supported
    *                      by the server.
    * @return the cache
    * @throws HotRodClientException
    */
   @Override
   <K, V> RemoteCache<K, V> getOrCreateCache(String name, BasicConfiguration configuration) throws HotRodClientException;

   /**
    * Creates a cache on the remote server cluster using the specified template and flags.
    *
    * @param name the name of the cache to create
    * @param template the template to use for the cache. If null, the configuration marked as default on the server
    *                 will be used
    * @param flags an {@link EnumSet} of flags to use when creating the cache. See {@link AdminFlag}
    * @throws HotRodClientException
    * @deprecated use {@link #withFlags(AdminFlag...)} instead
    */
   @Deprecated
   void createCache(String name, String template, EnumSet<org.infinispan.client.hotrod.AdminFlag> flags) throws HotRodClientException;

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
}
