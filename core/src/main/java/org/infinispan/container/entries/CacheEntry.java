package org.infinispan.container.entries;

import org.infinispan.metadata.Metadata;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.metadata.MetadataAware;

import java.util.Map;

/**
 * An entry that is stored in the data container
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
public interface CacheEntry extends Map.Entry<Object, Object>, MetadataAware {

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
    * @return true if this entry has been evicted since being read from the container, false otherwise.
    */
   boolean isEvicted();

   /**
    * @return true if this entry is still valid, false otherwise.
    */
   boolean isValid();

   /**
    * @return true if the entry was loaded from a cache store.
    */
   boolean isLoaded();

   /**
    * Retrieves the key to this entry
    *
    * @return a key
    */
   @Override
   Object getKey();

   /**
    * Retrieves the value of this entry
    *
    * @return the value of the entry
    */
   @Override
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
    * @return {@code true} if the value must not be fetch from an external source
    */
   boolean skipLookup();

   /**
    * Sets the value of the entry, returning the previous value
    *
    * @param value value to set
    * @return previous value
    */
   @Override
   Object setValue(Object value);

   /**
    * Commits changes
    *
    * @param container data container to commit to
    */
   void commit(DataContainer container, Metadata metadata);

   /**
    * Rolls back changes
    */
   void rollback();

   void setChanged(boolean changed);

   void setCreated(boolean created);

   void setRemoved(boolean removed);

   void setEvicted(boolean evicted);

   void setValid(boolean valid);

   void setLoaded(boolean loaded);

   /**
    * See {@link #skipLookup()}.
    * @param skipLookup
    */
   void setSkipLookup(boolean skipLookup);

   /**
    * If the entry is marked as removed and doUndelete==true then the "valid" flag is set to true and "removed"
    * flag is set to false.
    */
   boolean undelete(boolean doUndelete);
}
