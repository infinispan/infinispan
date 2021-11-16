package org.infinispan.security;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * AuthorizationPermission.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@ProtoTypeId(ProtoStreamTypeIds.AUTHORIZATION_PERMISSION)
public enum AuthorizationPermission {
   /**
    * Allows control of a cache's lifecycle (i.e. invoke {@link Cache#start()} and {@link Cache#stop()}
    */
   @ProtoEnumValue(value = 1 << 0, name = "LIFECYCLE_PERMISSION")
   LIFECYCLE(1 << 0),
   /**
    * Allows reading data from a cache
    */
   @ProtoEnumValue(1 << 1)
   READ(1 << 1),
   /**
    * Allows writing data to a cache
    */
   @ProtoEnumValue(1 << 2)
   WRITE(1 << 2),
   /**
    * Allows performing task execution (e.g. cluster executor, tasks) on a cache
    */
   @ProtoEnumValue(1 << 3)
   EXEC(1 << 3),
   /**
    * Allows attaching listeners to a cache
    */
   @ProtoEnumValue(1 << 4)
   LISTEN(1 << 4),
   /**
    * Allows bulk-read operations (e.g. {@link Cache#keySet()}) on a cache
    */
   @ProtoEnumValue(1 << 5)
   BULK_READ(1 << 5),
   /**
    * Allows bulk-write operations (e.g. {@link Cache#clear()}) on a cache
    */
   @ProtoEnumValue(1 << 6)
   BULK_WRITE(1 << 6),
   /**
    * Allows performing "administrative" operations on a cache
    */
   @ProtoEnumValue(1 << 7)
   ADMIN(1 << 7),
   /**
    * Allows creation of resources (caches, counters, schemas, tasks)
    */
   @ProtoEnumValue(1 << 8)
   CREATE(1 << 8),
   /**
    * Allows retrieval of stats
    */
   @ProtoEnumValue(1 << 9)
   MONITOR(1 << 9),
   /**
    * Aggregate permission which implies all the others
    */
   @ProtoEnumValue(Integer.MAX_VALUE)
   ALL(Integer.MAX_VALUE),
   /**
    * Aggregate permission which implies all read permissions
    */
   @ProtoEnumValue((1 << 1) + (1 << 5))
   ALL_READ(READ.getMask() + BULK_READ.getMask()),
   /**
    * Aggregate permission which implies all write permissions
    */
   @ProtoEnumValue((1 << 2) + (1 << 6))
   ALL_WRITE(WRITE.getMask() + BULK_WRITE.getMask()),
   /**
    * No permissions
    */
   @ProtoEnumValue
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
