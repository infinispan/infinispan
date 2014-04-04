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
    * Allows control of a cache's lifecycle (i.e. invoke {@link Cache#start()} and
    * {@link Cache#stop()}
    */
   LIFECYCLE(1 << 0),
   /**
    * Allows reading data from a cache
    */
   READ(1 << 1),
   /**
    * Allows writing data to a cache
    */
   WRITE(1 << 2),
   /**
    * Allows performing task execution (e.g. distributed executors, map/reduce) on a cache
    */
   EXEC(1 << 3),
   /**
    * Allows attaching listeners to a cache
    */
   LISTEN(1 << 4),
   /**
    * Allows bulk-read operations (e.g. {@link Cache#keySet()}) on a cache
    */
   BULK_READ(1 << 5),
   /**
    * Allows bulk-write operations (e.g. {@link Cache#clear()}) on a cache
    */
   BULK_WRITE(1 << 6),
   /**
    * Allows performing "administrative" operations on a cache
    */
   ADMIN(1 << 7),
   /**
    * Aggregate permission which implies all of the others
    */
   ALL(Integer.MAX_VALUE),
   /**
    * Aggregate permission which implies all read permissions
    */
   ALL_READ(READ.getMask() + BULK_READ.getMask()),
   /**
    * Aggregate permission which implies all write permissions
    */
   ALL_WRITE(WRITE.getMask() + BULK_WRITE.getMask()),
   /**
    * No permissions
    */
   NONE(0);

   private final int mask;
   private final CachePermission securityPermission;

   AuthorizationPermission(int mask) {
      this.mask = mask;
      securityPermission = new CachePermission(this);
   }

   public int getMask() {
      return mask;
   }

   public CachePermission getSecurityPermission() {
      return securityPermission;
   }

   public boolean matches(int mask) {
      return ((this.mask & mask) == this.mask);
   }

   public boolean implies(AuthorizationPermission that) {
      return ((this.mask & that.mask) == that.mask);
   }
}
