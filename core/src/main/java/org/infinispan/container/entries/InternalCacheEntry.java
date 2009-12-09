package org.infinispan.container.entries;

/**
 * Interface for internal cache entries that expose whether an entry has expired.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface InternalCacheEntry extends CacheEntry, Cloneable {
   /**
    * @return true if the entry has expired; false otherwise
    */
   boolean isExpired();

   /**
    * @return true if the entry can expire, false otherwise
    */
   boolean canExpire();

   /**
    * Sets the maximum idle time of the entry.
    *
    * @param maxIdle maxIdle to set
    */
   void setMaxIdle(long maxIdle);

   /**
    * Sets the lifespan of the entry.
    *
    * @param lifespan lifespan to set
    */
   void setLifespan(long lifespan);

   /**
    * @return timestamp when the entry was created
    */
   long getCreated();

   /**
    * @return timestamp when the entry was last used
    */
   long getLastUsed();

   /**
    * Only used with entries that have a lifespan, this determines when an entry is due to expire.
    *
    * @return timestamp when the entry is due to expire, or -1 if it doesn't have a lifespan
    */
   long getExpiryTime();

   /**
    * Updates access timestamps on this instance
    */
   void touch();

   /**
    * Creates a representation of this entry as an {@link org.infinispan.container.entries.InternalCacheValue}. The main
    * purpose of this is to provide a representation that does <i>not</i> have a reference to the key. This is useful in
    * situations where the key is already known or stored elsewhere, making serialization and deserialization more
    * efficient.
    * <p/>
    * Note that this should not be used to optimize memory overhead, since the saving of an additional reference to a
    * key (a single object reference) does not warrant the cost of constructing an InternalCacheValue.  This <i>only</i>
    * makes sense when marshalling is involved, since the cost of marshalling the key again can be sidestepped using an
    * InternalCacheValue if the key is already known/marshalled.
    * <p/>
    *
    * @return a new InternalCacheValue encapsulating this InternalCacheEntry's value and expiration information.
    */
   InternalCacheValue toInternalCacheValue();

   InternalCacheEntry clone();
}
