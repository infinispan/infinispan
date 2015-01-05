package org.infinispan.client.hotrod.event;

public class ClientEvents {

   private static final ClientCacheFailoverEvent FAILOVER_EVENT_SINGLETON = new ClientCacheFailoverEvent() {
      @Override
      public ClientEvent.Type getType() {
         return ClientEvent.Type.CLIENT_CACHE_FAILOVER;
      }
   };

   private ClientEvents() {
      // Static helper class, cannot be constructed
   }

   public static ClientCacheFailoverEvent mkCachefailoverEvent() {
      return FAILOVER_EVENT_SINGLETON;
   }

}
