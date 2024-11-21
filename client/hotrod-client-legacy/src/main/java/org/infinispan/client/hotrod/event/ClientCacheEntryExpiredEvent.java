package org.infinispan.client.hotrod.event;

/**
 * Client side cache entry expired events provide information on the expired key.
 *
 * @param <K> type of key expired.
 */
public interface ClientCacheEntryExpiredEvent<K> extends ClientEvent {

   /**
    * Created cache entry's key.
    * @return an instance of the key with which a cache entry has been
    * created in remote server.
    */
   K getKey();
}
