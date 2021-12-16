package org.infinispan.container.versioning.irac;

import java.util.Collection;

import org.infinispan.commands.irac.IracTombstoneCleanupCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.transport.Address;

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
   default void storeTombstoneIfAbsent(int segment, Object key, IracMetadata metadata) {
      if (metadata == null) {
         return;
      }
      storeTombstoneIfAbsent(new IracTombstoneInfo(key, segment, metadata));
   }

   /**
    * Same as {@link #storeTombstoneIfAbsent(int, Object, IracMetadata)} but with a {@link IracTombstoneInfo} instance.
    *
    * @param tombstone The tombstone to store.
    */
   void storeTombstoneIfAbsent(IracTombstoneInfo tombstone);

   /**
    * Removes the tombstone if it matches.
    *
    * @param tombstone The {@link IracTombstoneInfo}.
    */
   void removeTombstone(IracTombstoneInfo tombstone);

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

   /**
    * @return {@code true} if the cleanup task is currently running.
    */
   boolean isTaskRunning();

   /**
    * @return The current delay between cleanup task in milliseconds.
    */
   long getCurrentDelayMillis();

   /**
    * Sends the tombstone belonging to the segments in {@code segment} to the {@code originator}
    * <p>
    * The sending is done asynchronously, and it does not wait for the sending to complete.
    *
    * @param requestor The requestor {@link Address}.
    * @param segments  The segments requested.
    */
   void sendStateTo(Address requestor, IntSet segments);

   /**
    * It receives a {@link Collection} of {@link IracTombstoneInfo} and sends {@link IracTombstoneCleanupCommand} for
    * the tombstone no longer valid.
    *
    * @param tombstones The {@link IracTombstoneInfo} collection.
    */
   void checkStaleTombstone(Collection<? extends IracTombstoneInfo> tombstones);
}
