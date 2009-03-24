package org.horizon.container.entries;

/**
 * Interface for internal cache entries that expose whether an entry has expired.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface InternalCacheEntry extends CacheEntry {
   /**
    *
    * @return true if the entry has expired; false otherwise
    */
   boolean isExpired();

   /**
    * @return true if the entry can expire, false otherwise
    */
   boolean canExpire();

    /**
    * Sets the maximum idle time of the entry.
    * <p />
    * Note that if this method is used, you should always use a reference to the
    * return value after invocation, since as an optimization, implementations may change type of CacheEntry used after
    * invoking this method, for example changing a MortalCacheEntry to an ImmortalCacheEntry.
    * @param maxIdle maxIdle to set
    * @return the updated CacheEntry
    */
   InternalCacheEntry setMaxIdle(long maxIdle);

   /**
    * Sets the lifespan of the entry.
    * <p />
    * Note that if this method is used, you should always use a reference to the
    * return value after invocation, since as an optimization, implementations may change type of CacheEntry used after
    * invoking this method, for example changing a MortalCacheEntry to an ImmortalCacheEntry.
    * @param lifespan lifespan to set
    * @return the updated CacheEntry
    */
   InternalCacheEntry setLifespan(long lifespan);

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
    * @return timestamp when the entry is due to expire, or -1 if it doesn't have a lifespan
    */
   long getExpiryTime();

   /**
    * Updates access timestamps on this instance
    */
   void touch();
}
