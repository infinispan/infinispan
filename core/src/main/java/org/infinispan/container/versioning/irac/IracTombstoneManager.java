package org.infinispan.container.versioning.irac;

import org.infinispan.metadata.impl.IracMetadata;

/**
 * Stores and manages tombstones for removed keys.
 * <p>
 * It manages the tombstones for IRAC protocol. Tombstones are used when a key is removed but the version/metadata is
 * required to perform conflict or duplicates detection.
 * <p>
 * Tombstones are removed when they are not required by any site or its value is updated (with a non-null value).
 *
 * @since 14.0
 */
public interface IracTombstoneManager {

   /**
    * Stores a tombstone for a removed key.
    * <p>
    * It overwrites any previous tombstone associated to the {@code key}.
    *
    * @param key      The key.
    * @param segment  The key's segment.
    * @param metadata The {@link IracMetadata}.
    */
   void storeTombstone(int segment, Object key, IracMetadata metadata);

   /**
    * Same as {@link #storeTombstone(int, Object, IracMetadata)} but it doesn't overwrite an existing tombstone.
    *
    * @param key      The key.
    * @param segment  The key's segment.
    * @param metadata The {@link IracMetadata}.
    */
   void storeTombstoneIfAbsent(int segment, Object key, IracMetadata metadata);

   /**
    * Removes the tombstone for {@code key} if the metadata matches.
    *
    * @param key          The key.
    * @param iracMetadata The expected {@link IracMetadata}.
    */
   void removeTombstone(Object key, IracMetadata iracMetadata);

   /**
    * Removes the tombstone for {@code key}.
    *
    * @param key The key.
    */
   void removeTombstone(Object key);

   /**
    * Returns the tombstone associated to the {@code key} or {@code null} if it doesn't exist.
    *
    * @param key The key.
    * @return The tombstone.
    */
   IracMetadata getTombstone(Object key);

   /**
    * @return {@code true} if no tombstones are stored.
    */
   boolean isEmpty();

   /**
    * @return the number of tombstones stored.
    */
   int size();
}
