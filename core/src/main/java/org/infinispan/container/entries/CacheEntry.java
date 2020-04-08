package org.infinispan.container.entries;

import java.util.Map;

import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.metadata.MetadataAware;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * An entry that is stored in the data container
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
public interface CacheEntry<K, V> extends Cloneable, Map.Entry<K, V>, MetadataAware {

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
    * Retrieves the key to this entry
    *
    * @return a key
    */
   @Override
   K getKey();

   /**
    * Retrieves the value of this entry
    *
    * @return the value of the entry
    */
   @Override
   V getValue();

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
    * @return timestamp when the entry was created
    */
   default long getCreated() {
      return -1;
   }

   /**
    * @return timestamp when the entry was last used
    */
   default long getLastUsed() {
      return -1;
   }

   /**
    * Sets the value of the entry, returning the previous value
    *
    * @param value value to set
    * @return previous value
    */
   @Override
   V setValue(V value);

   /**
    * Commits changes
    *
    * @param container data container to commit to
    */
   void commit(DataContainer<K, V> container);

   void setChanged(boolean changed);

   void setCreated(boolean created);

   void setRemoved(boolean removed);

   void setEvicted(boolean evicted);

   /**
    * See {@link #skipLookup()}.
    * @param skipLookup
    */
   void setSkipLookup(boolean skipLookup);

   CacheEntry<K, V> clone();

   default void setCreated(long created) {}

   default void setLastUsed(long lastUsed) {}

   default PrivateMetadata getInternalMetadata() {
      return null;
   }

   default void setInternalMetadata(PrivateMetadata metadata) {
      //no-op
   }

}
