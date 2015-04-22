package org.infinispan.client.hotrod.event;

public interface ClientCacheEntryEvent<K> extends ClientEvent {
    /**
     * Cache entry's key.
     * @return an instance of the key corresponding to the cache entry
     */
    K getKey();
}
