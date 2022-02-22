package org.infinispan.api.sync;

import org.infinispan.api.configuration.CacheConfiguration;

/**
 * @since 14.0
 **/
public interface SyncCaches {
   /**
    * Obtains an existing cache
    *
    * @param name the name of the cache
    * @param <K> the type of the key
    * @param <V> the type of the value
    * @return
    */
   <K, V> SyncCache<K, V> get(String name);

   /**
    *
    * @param name the name of the cache
    * @param cacheConfiguration
    * @param <K> the type of the key
    * @param <V> the type of the value
    * @return
    */
   <K, V> SyncCache<K, V> create(String name, CacheConfiguration cacheConfiguration);

   /**
    * Creates a cache using the supplied template name
    *
    * @param name the name of the cache
    * @param template the name of an existing template
    * @param <K> the type of the key
    * @param <V> the type of the value
    * @return
    */
   <K, V> SyncCache<K, V> create(String name, String template);

   /**
    * Removes a cache
    *
    * @param name the name of the cache to be removed
    */
   void remove(String name);

   /**
    * Retrieves the names of all available caches
    *
    * @return
    */
   Iterable<String> names();

   /**
    * Creates a cache template
    *
    * @param name the name of the template
    * @param cacheConfiguration the configuration of the template
    */
   void createTemplate(String name, CacheConfiguration cacheConfiguration);

   /**
    * Removes a cache template
    *
    * @param name the name of the template to be removed
    */
   void removeTemplate(String name);

   /**
    * Returns the names of all available templates
    *
    * @return
    */
   Iterable<String> templateNames();
}
