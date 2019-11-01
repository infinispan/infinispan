package org.infinispan.configuration.cache;

/**
 * Used to configure indexing of entries in the cache for index-assisted searching.
 *
 * @author Paul Ferraro
 */
public enum Index {

   /**
    * No indexing is performed.
    */
   NONE,

   /**
    * All cluster nodes will add the entry to the index.
    */
   ALL,

   /**
    * Only the primary owner of an entry will add it to the index.
    */
   PRIMARY_OWNER;

   /**
    * Is indexing enabled?
    */
   public boolean isEnabled() {
      return this != NONE;
   }

   /**
    * @deprecated in 10.1. Equivalent to a simple equality comparison to {@link #PRIMARY_OWNER}.
    */
   @Deprecated
   public boolean isPrimaryOwner() {
      return this == PRIMARY_OWNER;
   }
}
