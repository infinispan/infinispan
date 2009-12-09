package org.infinispan.container.entries;

import org.infinispan.container.DataContainer;

import java.util.Map;

/**
 * An entry that is stored in the data container
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
public interface CacheEntry extends Map.Entry<Object, Object> {

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
