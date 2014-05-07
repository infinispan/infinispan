package org.infinispan.client.hotrod.event;

/**
 * @author Galder Zamarreño
 */
public interface ClientCacheEntryModifiedEvent<K> extends ClientEvent {

   K getKey();

   long getVersion();

}
