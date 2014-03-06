package org.infinispan.container.entries;

/**
 * Interface for internal cache entries that expose whether an entry has expired.
 *
 * @author Manik Surtani
 * @author Sanne Grinovero
 * @since 4.0
 */
public interface InternalCacheEntry<K, V> extends CacheEntry<K, V>, Cloneable {

   /**
    * @param now the current time as defined by {@link System#currentTimeMillis()} or {@link
    *            org.infinispan.util.TimeService#wallClockTime()}
    * @return true if the entry has expired; false otherwise
    * @since 5.1
    */
   boolean isExpired(long now);

   /**
    * @return true if the entry has expired; false otherwise
    * @deprecated use {@link #isExpired(long)}
    */
   @Deprecated
   boolean isExpired();

   /**
    * @return true if the entry can expire, false otherwise
    */
   boolean canExpire();

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
    * @deprecated use {@link #touch(long)}
    */
   @Deprecated
   void touch();

   /**
    * Updates access timestamps on this instance to a specified time
    * @param currentTimeMillis the current time as defined by {@link System#currentTimeMillis()} or {@link
    *                          org.infinispan.util.TimeService#wallClockTime()}
    */
   void touch(long currentTimeMillis);

   /**
    * "Reincarnates" an entry.  Essentially, resets the 'created' timestamp of the entry to the current time.
    * @deprecated use {@link #reincarnate(long)}
    */
   @Deprecated
   void reincarnate();

   /**
    * "Reincarnates" an entry.  Essentially, resets the 'created' timestamp of the entry to the current time.
    * @param now the current time as defined by {@link System#currentTimeMillis()} or {@link
    *            org.infinispan.util.TimeService#wallClockTime()}
    */
   void reincarnate(long now);

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
   InternalCacheValue<V> toInternalCacheValue();

   InternalCacheEntry<K, V> clone();
}
