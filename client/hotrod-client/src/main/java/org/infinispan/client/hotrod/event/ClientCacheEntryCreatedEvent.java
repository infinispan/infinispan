package org.infinispan.client.hotrod.event;

/**
 * @author Galder Zamarreño
 */
public interface ClientCacheEntryCreatedEvent<K> extends ClientEvent {

   K getKey();

   long getVersion();

}
