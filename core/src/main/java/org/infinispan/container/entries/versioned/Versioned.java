package org.infinispan.container.entries.versioned;

import org.infinispan.container.versioning.EntryVersion;

/**
 * An interface that marks the ability to handle versions
 *
 * @author Manik Surtani
 * @since 5.1
 */
public interface Versioned {

   /**
    * @return the version of the entry.  May be null if versioning is not supported, and must never be null if
    *         versioning is supported.
    */
   EntryVersion getVersion();

   /**
    * Sets the version on this entry.
    *
    * @param version version to set
    */
   void setVersion(EntryVersion version);
}
