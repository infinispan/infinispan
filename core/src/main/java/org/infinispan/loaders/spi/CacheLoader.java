package org.infinispan.loaders.spi;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.loaders.CacheLoaderException;

import java.util.Set;

/**
 * Responsible for loading cache data from an external source
 *
 * @author Manik Surtani
 * @author Navin Surtani
 * @author Tristan Tarrant
 * @since 6.0
 */
public interface CacheLoader {

   /**
    * Used to initialize a cache loader.  Typically invoked by the {@link org.infinispan.loaders.manager.CacheLoaderManager}
    * when setting up cache loaders.
    *
    * @param configuration the cache loader configuration bean
    * @param cache  cache associated with this cache loader. Implementations may use this to determine cache name when
    *               selecting where refer to state in storage, for example, a different database table name.
    * @param m      marshaller to use when loading state from a stream, if supported by the implementation.
    */
   void init(CacheLoaderConfiguration configuration, Cache<?, ?> cache, StreamingMarshaller m) throws
         CacheLoaderException;

   /**
    * Loads an entry mapped to by a given key.  Should return null if the entry does not exist.  Expired entries are not
    * returned.
    *
    * @param key key
    * @return an entry
    * @throws CacheLoaderException in the event of problems reading from source
    */
   InternalCacheEntry load(Object key) throws CacheLoaderException;

   /**
    * Loads all entries in the loader.  Expired entries are not returned.
    *
    * @return a set of entries, or an empty set if the loader is emptied.
    * @throws CacheLoaderException in the event of problems reading from source
    */
   Set<InternalCacheEntry> loadAll() throws CacheLoaderException;

   /**
    * Loads up to a specific number of entries.  There is no guarantee as to order of entries loaded.  The set returned
    * would contain up to a maximum of <tt>numEntries</tt> entries, and no more.
    * @param numEntries maximum number of entries to load
    * @return a set of entries, which would contain between 0 and numEntries entries.
    * @throws CacheLoaderException
    */
   Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException;

   /**
    * Loads a set of all keys, excluding a filter set.
    *
    * @param keysToExclude a set of keys to exclude.  An empty set or null will indicate that all keys should be returned.
    * @return A set containing keys of entries stored.  An empty set is returned if the loader is empty.
    * @throws CacheLoaderException
    */
   Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException;

   /**
    * @param key key to test
    * @return true if the key exists, false otherwise
    * @throws CacheLoaderException in the event of problems reading from source
    */
   boolean containsKey(Object key) throws CacheLoaderException;

   public void start() throws CacheLoaderException;

   public void stop() throws CacheLoaderException;

   /**
    * Obtain the {@link org.infinispan.configuration.cache.CacheStoreConfiguration} related to the CacheLoader.
    *
    * @return the {@link org.infinispan.configuration.cache.CacheStoreConfiguration} used.
    */
   CacheLoaderConfiguration getConfiguration();
}
