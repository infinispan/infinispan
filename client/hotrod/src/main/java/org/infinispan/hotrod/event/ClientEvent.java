package org.infinispan.hotrod.event;

/**
 */
public interface ClientEvent {

   enum Type {
      CLIENT_CACHE_ENTRY_CREATED,
      CLIENT_CACHE_ENTRY_MODIFIED,
      CLIENT_CACHE_ENTRY_REMOVED,
      CLIENT_CACHE_ENTRY_EXPIRED,
      CLIENT_CACHE_FAILOVER
   }

   Type getType();
}
