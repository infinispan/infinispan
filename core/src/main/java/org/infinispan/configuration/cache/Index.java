package org.infinispan.configuration.cache;

/**
 * Used to configure indexing mode of entries in the cache for index-assisted searching.
 * <p>As of version 11 only modes {@link #NONE} and {@link #ALL} are actually internally supported and the indexing
 * mode no longer needs to be specified by the user because the system will determine it automatically.
 *
 * @author Paul Ferraro
 * @deprecated since 11.0. The indexing mode is automatically detected and not configurable anymore (ignored) and will be
 * completely removed in next major version.
 */
@Deprecated(forRemoval = true)
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
    *
    * @deprecated This mode is no longer supported since version 11.0. A configuration error will be raised if encountered.
    */
   @Deprecated(forRemoval = true)
   PRIMARY_OWNER;

   /**
    * Is indexing enabled?
    *
    * @deprecated Use {@link IndexingConfiguration#enabled()}} instead
    */
   public boolean isEnabled() {
      return this != NONE;
   }

   /**
    * @deprecated in 10.1. Equivalent to a simple equality comparison to {@link #PRIMARY_OWNER}.
    */
   public boolean isPrimaryOwner() {
      return this == PRIMARY_OWNER;
   }
}
