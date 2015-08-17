package org.infinispan.container.entries;

import org.infinispan.container.DataContainer;

/**
 * An entry that can be safely copied when updates are made, to provide MVCC semantics
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface MVCCEntry<K, V> extends CacheEntry<K, V>, StateChangingEntry {

   /**
    * Makes internal copies of the entry for updates
    *
    * @param container      data container
    */
   void copyForUpdate(DataContainer<? super K, ? super V> container);

   void setChanged(boolean isChanged);

   /**
    * Marks this entry as being expired.  This is a special form of removal.
    * @param expired whether or not this entry should be expired
    */
   void setExpired(boolean expired);

   /**
    * Returns whether this entry was marked as being expired or not
    * @return whether expired has been set
    */
   boolean isExpired();
}
