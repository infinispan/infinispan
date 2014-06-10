package org.infinispan.client.hotrod.event;

/**
 * @author Galder Zamarreño
 */
public interface ClientCacheEntryRemovedEvent<K> extends ClientEvent {

   K getKey();

}
