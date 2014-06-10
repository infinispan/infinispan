package org.infinispan.client.hotrod.event;

/**
 * @author Galder Zamarreño
 */
public interface ClientEvent {
   static enum Type {
      CLIENT_CACHE_ENTRY_CREATED, CLIENT_CACHE_ENTRY_MODIFIED,
      CLIENT_CACHE_ENTRY_REMOVED, CLIENT_CACHE_ENTRY_CUSTOM
   }

   Type getType();
}
