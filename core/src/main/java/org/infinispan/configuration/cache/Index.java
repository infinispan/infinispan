package org.infinispan.configuration.cache;

/**
 * Used to configure indexing of entries in the cache for searching.
 *
 * @author Paul Ferraro
 */
public enum Index {
   NONE, LOCAL, ALL, PRIMARY_OWNER;

   public boolean isEnabled() {
      return this != NONE;
   }

   public boolean isLocalOnly() {
      return this == LOCAL;
   }

   public boolean isPrimaryOwner() {
      return this == PRIMARY_OWNER;
   }

}
