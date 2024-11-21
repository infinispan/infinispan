package org.infinispan.client.hotrod.configuration;

/**
 * Decides how client-side near caching should work.
 *
 * @since 7.1
 */
public enum NearCacheMode {

   // TODO: Add SELECTIVE (or similar) when ISPN-5545 implemented

   /**
    * Near caching is disabled.
    */
   DISABLED,

   /**
    * Near cache is invalidated, so when entries are updated or removed
    * server-side, invalidation messages will be sent to clients to remove
    * them from the near cache.
    */
   INVALIDATED;

   public boolean enabled() {
      return this != DISABLED;
   }

   public boolean invalidated() {
      return this == INVALIDATED;
   }

}
