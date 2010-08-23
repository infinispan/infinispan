package org.infinispan.container.entries;

import org.infinispan.container.DataContainer;

/**
 * An entry that can be safely copied when updates are made, to provide MVCC semantics
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface MVCCEntry extends CacheEntry {

   /**
    * Makes internal copies of the entry for updates
    *
    * @param container      data container
    * @param writeSkewCheck if true, write skews are tested for and exceptions are thrown if detected.  Only applicable
    *                       to {@link org.infinispan.util.concurrent.IsolationLevel#REPEATABLE_READ}.
    */
   void copyForUpdate(DataContainer container, boolean writeSkewCheck);
}
