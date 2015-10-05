package org.infinispan.stream;

import org.infinispan.Cache;

/**
 * Interface that describes how a cache can be injected into another object.  This is useful for cases such as
 * after an object is deserialized and you must inject a Cache into it.
 * @since 8.1
 */
public interface CacheAware<K, V> {
   /**
    * Method that is invoked when a cache is to be injected.
    * @param cache the cache instance tied to the object
    */
   void injectCache(Cache<K, V> cache);
}
