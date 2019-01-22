package org.infinispan.api;

import org.infinispan.api.collections.reactive.KeyValueStore;

/**
 * Each infinispan is a node instance. It can be embedded or client node, depending on the access point. {@link
 * InfinispanClient} or {@link InfinispanEmbedded}
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
public interface Infinispan {

   /**
    * Returns a reactive cache instance with the specified name
    *
    * @param name, name of the cache
    * @return, the reactive cache instance
    */
   <K, V> KeyValueStore<K, V> getKeyValueStore(String name);

}
