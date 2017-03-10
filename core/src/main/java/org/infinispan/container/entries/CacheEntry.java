package org.infinispan.container.entries;

import java.util.Map;

import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.metadata.MetadataAware;
import org.infinispan.metadata.Metadata;

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
    * @deprecated since 9.2
    */
   @Deprecated
   default boolean isValid() {
      throw new UnsupportedOperationException();
   }

   /**
    * @deprecated since 8.1
    */
   @Deprecated
   default boolean isLoaded() {
      throw new UnsupportedOperationException();
   }

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
    * @deprecated since 9.1
    */
   @Deprecated
   default void commit(DataContainer<K, V> container, Metadata metadata) {
      commit(container);
   }

   /**
    * Commits changes
    *
    * @param container data container to commit to
    */
   void commit(DataContainer<K, V> container);

   /**
    * Rolls back changes
    *
    * @deprecated since 9.0
    */
   @Deprecated
   default void rollback() {}

   void setChanged(boolean changed);

   void setCreated(boolean created);

   void setRemoved(boolean removed);

   void setEvicted(boolean evicted);

   /**
    * @deprecated since 9.2
    */
   @Deprecated
   default void setValid(boolean valid) {
   }

   /**
    * @deprecated since 8.1
    * Note: Since 9.2 moved to {@link MVCCEntry}
    */
   @Deprecated
   default void setLoaded(boolean loaded) {
   }

   /**
    * See {@link #skipLookup()}.
    * @param skipLookup
    */
   void setSkipLookup(boolean skipLookup);

   /**
    * If the entry is marked as removed and doUndelete==true then the "valid" flag is set to true and "removed"
    * flag is set to false.
    */
   @Deprecated
   default boolean undelete(boolean doUndelete) {
      return false;
   }

   CacheEntry<K, V> clone();

   default void setCreated(long created) {}

   default void setLastUsed(long lastUsed) {}

}
