package org.infinispan.client.hotrod.multimap;

import org.infinispan.commons.util.Experimental;

@Experimental
public interface MultimapCacheManager<K, V> {

   /**
    * Retrieves a named multimap cache from the system.
    *
    * @param name, name of multimap cache to retrieve
    * @return null if no configuration exists as per rules set above, otherwise returns a multimap cache instance
    * identified by cacheName
    */
   RemoteMultimapCache<K, V> get(String name);
}
