package org.infinispan.security;

import org.infinispan.Cache;

/**
 * AuthorizationPermission.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public enum AuthorizationPermission {
   /**
    * Allows control of a cache's lifecycle (i.e. invoke {@link Cache#start()} and {@link Cache#stop()}
    */
   LIFECYCLE,
   /**
    * Allows reading data from a cache
    */
   READ,
   /**
    * Allows writing data to a cache
    */
   WRITE,
   /**
    * Allows performing task execution (e.g. distributed executors, map/reduce) on a cache
    */
   EXEC,
   /**
    * Allows attaching listeners to a cache
    */
   LISTEN,
   /**
    * Allows bulk-read operations (e.g. {@link Cache#keySet()}) on a cache
    */
   BULK_READ,
   /**
    * Allows bulk-write operations (e.g. {@link Cache#clear()}) on a cache
    */
   BULK_WRITE,
   /**
    * Allows performing "administrative" operations on a cache
    */
   ADMIN,
   /**
    * Synthetic permission which implies all of the others
    */
   ALL(Integer.MAX_VALUE),
   /**
    * Synthetic permission which means no permissions
    */
   NONE(0);

   private final int mask;

   AuthorizationPermission() {
      this.mask = 1 << ordinal();
   }

   AuthorizationPermission(int mask) {
      this.mask = mask;
   }

   public int getMask() {
      return mask;
   }

   public boolean matches(int mask) {
      return ((this.mask & mask) == this.mask);
   }
}
