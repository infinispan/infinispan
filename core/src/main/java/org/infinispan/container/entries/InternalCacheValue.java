package org.infinispan.container.entries;

import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;

/**
 * A representation of an InternalCacheEntry that does not have a reference to the key.  This should be used if the key
 * is either not needed or available elsewhere as it is more efficient to marshall and unmarshall.  Probably most useful
 * in cache stores.
 * <p/>
 * Note that this should not be used to optimize memory overhead, since the saving of an additional reference to a key
 * (a single object reference) does not warrant the cost of constructing an InternalCacheValue, where an existing
 * InternalCacheEntry is already referenced.
 * <p/>
 * Use of this interface <i>only</i> makes sense when marshalling is involved, since the cost of marshalling the key
 * again can be sidestepped using an InternalCacheValue if the key is already known/marshalled.
 * <p/>
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface InternalCacheValue {

   /**
    * @return the value represented by this internal wrapper
    */
   Object getValue();

   InternalCacheEntry toInternalCacheEntry(Object key);

   /**
    * @param now the current time as expressed by {@link System#currentTimeMillis()}
    * @return true if the entry has expired; false otherwise
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
    * @return lifespan of the value
    */
   long getLifespan();

   /**
    * @return max idle time allowed
    */
   long getMaxIdle();

   public long getExpiryTime();

   Metadata getMetadata();
}
