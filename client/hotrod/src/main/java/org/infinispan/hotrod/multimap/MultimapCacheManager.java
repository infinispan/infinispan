package org.infinispan.hotrod.multimap;

import org.infinispan.commons.util.Experimental;

@Experimental
public interface MultimapCacheManager<K, V> {

   /**
    * Retrieves a named multimap cache from the system.
    *
    * @param name, name of multimap cache to retrieve
    * @return null if no configuration exists as per rules set above, otherwise returns a multimap cache instance
    * identified by cacheName and doesn't support duplicates
    */
   default RemoteMultimapCache<K, V> get(String name) {
      return get(name, false);
   }

   /**
    * Retrieves a named multimap cache from the system.
    *
    * @param name, name of multimap cache to retrieve
    * @param supportsDuplicates, boolean check for identifying whether it supports duplicates or not.
    * @return null if no configuration exists as per rules set above, otherwise returns a multimap cache instance
    * identified by cacheName
    */
   RemoteMultimapCache<K, V> get(String name, boolean supportsDuplicates);
}
