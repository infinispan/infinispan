package org.infinispan.configuration.cache;

/**
 * Used to configure indexing of entries in the cache for searching.
 *
 * @author Paul Ferraro
 */
public enum Index {
   NONE,
   /**
    * Use PRIMARY_OWNER instead
    * @deprecated since 9.1
    */
   @Deprecated LOCAL,
   ALL,
   PRIMARY_OWNER;

   public boolean isEnabled() {
      return this != NONE;
   }

   @Deprecated
   public boolean isLocalOnly() {
      return this == LOCAL;
   }

   public boolean isPrimaryOwner() {
      return this == PRIMARY_OWNER;
   }

}
