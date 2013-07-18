package org.infinispan.container.entries;

import org.infinispan.container.DataContainer;

/**
 * An entry that can be safely copied when updates are made, to provide MVCC semantics
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface MVCCEntry extends CacheEntry, StateChangingEntry {

   /**
    * Makes internal copies of the entry for updates
    *
    * @param container      data container
    */
   void copyForUpdate(DataContainer container);

   void setChanged(boolean isChanged);
}
