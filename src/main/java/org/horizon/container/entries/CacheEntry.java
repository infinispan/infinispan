package org.horizon.container.entries;

import org.horizon.container.DataContainer;

import java.io.Serializable;

/**
 * An entry that is stored in the data container
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheEntry extends Serializable {

   /**
    * Tests whether the entry represents a null value, typically used for repeatable read.
    *
    * @return true if this represents a null, false otherwise.
    */
   boolean isNull();

   /**
    * @return true if this entry has changed since being read from the container, false otherwise.
    */
   boolean isChanged();

   /**
    * @return true if this entry has been newly created, false otherwise.
    */
   boolean isCreated();

   /**
    * @return true if this entry has been removed since being read from the container, false otherwise.
    */
   boolean isRemoved();

   /**
    * @return true if this entry is still valid, false otherwise.
    */
   boolean isValid();

   /**
    * Retrieves the key to this entry
    *
    * @return a key
    */
   Object getKey();

   /**
    * Retrieves the value of this entry
    *
    * @return the value of the entry
    */
   Object getValue();

   /**
    * @return retrieves the lifespan of this entry.  -1 means an unlimited lifespan.
    */
   long getLifespan();

   /**
    * @return the maximum allowed time for which this entry can be idle, after which it is considered expired.
    */
   long getMaxIdle();

   /**
    * Sets the maximum idle time of the entry.
    * <p/>
    * Note that if this method is used, you should always use a reference to the return value after invocation, since as
    * an optimization, implementations may change type of CacheEntry used after invoking this method, for example
    * changing a MortalCacheEntry to an ImmortalCacheEntry.
    *
    * @param maxIdle maxIdle to set
    * @return the updated CacheEntry
    */
   CacheEntry setMaxIdle(long maxIdle);

   /**
    * Sets the lifespan of the entry.
    * <p/>
    * Note that if this method is used, you should always use a reference to the return value after invocation, since as
    * an optimization, implementations may change type of CacheEntry used after invoking this method, for example
    * changing a MortalCacheEntry to an ImmortalCacheEntry.
    *
    * @param lifespan lifespan to set
    * @return the updated CacheEntry
    */
   CacheEntry setLifespan(long lifespan);

   /**
    * Sets the value of the entry, returing the previous value
    *
    * @param value value to set
    * @return previous value
    */
   Object setValue(Object value);

   /**
    * Commits changes
    *
    * @param container data container to commit to
    */
   void commit(DataContainer container);

   /**
    * Rolls back changes
    */
   void rollback();

   void setCreated(boolean created);

   void setRemoved(boolean removed);

   void setValid(boolean valid);
}
