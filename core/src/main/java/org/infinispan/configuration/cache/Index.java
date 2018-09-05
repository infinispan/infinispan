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
    * The local (originator) node is responsible for adding the entry to the index.
    * This indexing mode is deprecated; use {@link #PRIMARY_OWNER} instead.
    * @deprecated since 9.1
    */
   @Deprecated
   LOCAL,

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

   @Deprecated
   public boolean isLocalOnly() {
      return this == LOCAL;
   }

   public boolean isPrimaryOwner() {
      return this == PRIMARY_OWNER;
   }
}
