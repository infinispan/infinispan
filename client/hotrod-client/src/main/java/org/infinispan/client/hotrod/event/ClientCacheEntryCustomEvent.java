package org.infinispan.client.hotrod.event;

public interface ClientCacheEntryCustomEvent<T> extends ClientEvent {

   T getEventData();

}
