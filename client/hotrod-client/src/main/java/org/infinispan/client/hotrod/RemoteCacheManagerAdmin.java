package org.infinispan.client.hotrod;

import java.util.EnumSet;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;

/**
 * Remote Administration operations
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public interface RemoteCacheManagerAdmin {

   /**
    * Creates a cache on the remote server cluster using the specified template.
    *
    * @param name the name of the cache to create
    * @param template the template to use for the cache. If null, the configuration marked as default on the server
    *                 will be used
    * @throws HotRodClientException
    */
   void createCache(String name, String template) throws HotRodClientException;

   /**
    * Creates a cache on the remote server cluster using the specified template and flags.
    *
    * @param name the name of the cache to create
    * @param template the template to use for the cache. If null, the configuration marked as default on the server
    *                 will be used
    * @param flags an {@link EnumSet} of flags to use when creating the cache. See {@link AdminFlag}
    * @throws HotRodClientException
    */
   void createCache(String name, String template, EnumSet<AdminFlag> flags) throws HotRodClientException;

   /**
    * Removes a cache from the remote server cluster.
    *
    * @param name the name of the cache to remove
    * @throws HotRodClientException
    */
   void removeCache(String name) throws HotRodClientException;

   /**
    * Performs a mass reindexing of the specified cache. The command will return immediately and the reindexing will
    * be performed asynchronously
    * @param name the name of the cache to reindex
    * @throws HotRodClientException
    */
   void reindexCache(String name) throws HotRodClientException;
}
