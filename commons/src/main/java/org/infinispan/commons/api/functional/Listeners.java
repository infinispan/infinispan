package org.infinispan.commons.api.functional;

import org.infinispan.commons.api.functional.EntryView.ReadEntryView;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Listener definitions.
 *
 * DESIGN RATIONALE:
 * <ul>
 *    <li>The current set of listener events that can be fired are related
 *    to modifications, and deciding between create or modify, or providing
 *    removed entry information, require reading the previous value. Hence,
 *    the current set of listeners can only be provided for read-write maps.
 *    In the future, if for example cache entry visited events are to be
 *    supported, those would need to be associated with either a read or
 *    read-write map, and hence the listeners interface would most likely
 *    be split up.
 *    </li>
 * </ul>
 *
 * @since 8.0
 */
public final class Listeners {

   private Listeners() {
      // Cannot be instantiated, it's just a holder class
   }

   /**
    * Read-write listeners enables user to register listeners for events
    * happening in the read-write functional map.
    *
    * DESIGN RATIONALES:
    * <ul>
    *    <li>Read-write listeners can distinguish between cache entry
    *    creation versus cache entry modification or cache entry update
    *    (a better name btw!). This is because it can read the previous
    *    value and find out whether when the cache entry was written, whether
    *    the previous cache entry was present or not. {@link WriteListeners}
    *    cannot make such distinction because they are not allowed to read
    *    the previous value.
    *    </li>
    *    <li>Read-write listeners can find out about entry removed events
    *    and what the previous value for the entry was. Write-only listeners
    *    do not have such capability since once again, they have no access to
    *    previously stored data.
    *    </li>
    * </ul>
    */
   public interface ReadWriteListeners<K, V> {
      /**
       * Add a create event specific listener by passing in a
       * {@link Consumer} to be called back each time a new cache entry is
       * created, passing in a {@link ReadEntryView} of that entry.
       *
       * This method is shortcut for users who are only interested in
       * create events. If interested in multiple event types, calling
       * {@link #add(ReadWriteListener)} is recommended instead.
       *
       * @param f operation to be called each time a new cache entry is created
       * @return an {@link AutoCloseable} instance that can be used to
       *         unregister the listener
       */
      AutoCloseable onCreate(Consumer<ReadEntryView<K, V>> f);

      /**
       * Add a modify/update event specific listener by passing in a
       * {@link BiConsumer} to be called back each time an entry is
       * modified or updated, passing in a {@link ReadEntryView} of the
       * previous entry as first parameter, and a {@link ReadEntryView} of the
       * new value as second parameter.
       *
       * This method is shortcut for users who are only interested in
       * update events. If interested in multiple event types, calling
       * {@link #add(ReadWriteListener)} is recommended instead.
       *
       * @param f operation to be called each time a new cache entry is modified or updated,
       *          with the first parameter the {@link ReadEntryView} of the previous
       *          entry value, and the second parameter the new {@link ReadEntryView}
       * @return an {@link AutoCloseable} instance that can be used to
       *         unregister the listener
       */
      AutoCloseable onModify(BiConsumer<ReadEntryView<K, V>, ReadEntryView<K, V>> f);

      /**
       * Add a remove event specific listener by passing in a
       * {@link Consumer} to be called back each time an entry is
       * removed, passing in the {@link ReadEntryView} of the removed entry.
       *
       * This method is shortcut for users who are only interested in
       * remove events. If interested in multiple event types, calling
       * {@link #add(ReadWriteListener)} is recommended instead.
       *
       * @param f operation to be called each time a new cache entry is removed,
       *          with the old cached entry's {@link ReadEntryView} as parameter.
       * @return an {@link AutoCloseable} instance that can be used to
       *         unregister the listener
       */
      AutoCloseable onRemove(Consumer<ReadEntryView<K, V>> f);

      /**
       * Add a read-write listener, and return an {@link AutoCloseable}
       * instance that can be used to remove the listener registration.
       *
       * @param l the read-write functional map event listener
       * @return an {@link AutoCloseable} instance that can be used to
       *         unregister the listener
       */
      AutoCloseable add(ReadWriteListener<K, V> l);

      /**
       * Read-write listener
       */
      interface ReadWriteListener<K, V> {
         /**
          * Entry created event callback that receives a {@link ReadEntryView}
          * of the created entry.
          *
          * @param created created entry view
          */
         default void onCreate(ReadEntryView<K, V> created) {}

         /**
          * Entry modify/update event callback that receives {@link ReadEntryView}
          * of the previous entry as first parameter, and the {@link ReadEntryView}
          * of the new entry.
          *
          * @param before previous entry view
          * @param after new entry view
          */
         default void onModify(ReadEntryView<K, V> before, ReadEntryView<K, V> after) {}

         /**
          * Entry removed event callback that receives a {@link ReadEntryView}
          * of the removed entry.
          *
          * @param removed removed entry view
          */
         default void onRemove(ReadEntryView<K, V> removed) {}
      }
   }

   /**
    * Write-only listeners enables user to register listeners for events
    * happening in the write-only functional map.
    *
    * DESIGN RATIONALES:
    * <ul>
    *    <li>Write-only listeners cannot distinguish between cache entry
    *    created and cache entry modify/update events. In either case, all
    *    they know is that a new entry has been added, but they can't know if
    *    entry was present before or not, nor what the previous value.
    *    </li>
    *    <li>Write-only listeners can distinguish between entry removals
    *    vs cache entry create/modify-update events because they can query
    *    what the new entry's value via {@link ReadEntryView#find()}.
    *    </li>
    * </ul>
    */
   public interface WriteListeners<K, V> {
      /**
       * Add a write event listener by passing in a {@link Consumer} to be
       * called each time a cache entry is created, modified/updated or
       * removed.
       *
       * For created or modified/updated events, the
       * {@link ReadEntryView} passed in will represent the newly stored
       * entry, hence implementations will not be available to differentiate
       * between created events vs modified/updated events.
       *
       * For removed events, {@link ReadEntryView} passed in will represent
       * an empty entry view, hence {@link ReadEntryView#find()} will return
       * an {@link java.util.Optional} instance that's empty, and
       * {@link ReadEntryView#get()} will throw
       * {@link java.util.NoSuchElementException}.
       *
       * @param f operation to be called each time a cache entry is written
       * @return an {@link AutoCloseable} instance that can be used to
       *         unregister the listener
       */
      AutoCloseable onWrite(Consumer<ReadEntryView<K, V>> f);

      /**
       * Add a write-only listener, and return an {@link AutoCloseable}
       * instance that can be used to remove the listener registration.
       *
       * @param l the write-only functional map event listener
       * @return an {@link AutoCloseable} instance that can be used to
       *         unregister the listener
       */
      AutoCloseable add(WriteListener<K, V> l);

      /**
       * Write-only listener.
       */
      interface WriteListener<K, V> {
         /**
          * Entry write event callback that receives a {@link ReadEntryView}
          * of the written entry.
          *
          * For created or modified/updated events, the
          * {@link ReadEntryView} passed in will represent the newly stored
          * entry, hence implementations will not be available to differentiate
          * between created events vs modified/updated events.
          *
          * For removed events, {@link ReadEntryView} passed in will represent
          * an empty entry view, hence {@link ReadEntryView#find()} will return
          * an {@link java.util.Optional} instance that's empty, and
          * {@link ReadEntryView#get()} will throw
          * {@link java.util.NoSuchElementException}.
          *
          * @param write written entry view
          */
         void onWrite(ReadEntryView<K, V> write);
      }
   }

}
