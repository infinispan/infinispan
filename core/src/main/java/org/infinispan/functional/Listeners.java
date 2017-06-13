package org.infinispan.functional;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.commons.util.Experimental;

/**
 * Holder class for functional listener definitions.
 *
 * @since 8.0
 */
@Experimental
public final class Listeners {

   private Listeners() {
      // Cannot be instantiated, it's just a holder class
   }

   /**
    * Read-write listeners enable users to register listeners for cache
    * entry created, modified and removed events, and also register listeners
    * for any cache entry write events.
    *
    * <p>Entry created, modified and removed events can only be fired when these
    * originate on a read-write functional map, since this is the only one
    * that guarantees that the previous value has been read, and hence the
    * differentiation between create, modified and removed can be fully
    * guaranteed.
    *
    * @since 8.0
    */
   @Experimental
   public interface ReadWriteListeners<K, V> extends WriteListeners<K, V> {
      /**
       * Add a create event specific listener by passing in a
       * {@link Consumer} to be called back each time a new cache entry is
       * created, passing in a {@link ReadEntryView} of that new entry.
       *
       * <p>This method is shortcut for users who are only interested in
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
       * <p>This method is shortcut for users who are only interested in
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
       * <p>This method is shortcut for users who are only interested in
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
    * Write listeners enable user to register listeners for any cache entry
    * write events that happen in either a read-write or write-only
    * functional map.
    *
    * <p>Listeners for write events cannot distinguish between cache entry
    * created and cache entry modify/update events because they don't have
    * access to the previous value. All they know is that a new non-null
    * entry has been written.
    *
    * <p>However, write event listeners can distinguish between entry removals
    * and cache entry create/modify-update events because they can query
    * what the new entry's value via {@link ReadEntryView#find()}.
    *
    * @since 8.0
    */
   @Experimental
   public interface WriteListeners<K, V> {
      /**
       * Add a write event listener by passing in a {@link Consumer} to be
       * called each time a cache entry is created, modified/updated or
       * removed.
       *
       * <p>For created or modified/updated events, the
       * {@link ReadEntryView} passed in will represent the newly stored
       * entry, hence implementations will not be available to differentiate
       * between created events vs modified/updated events.
       *
       * <p>For removed events, {@link ReadEntryView} passed in will represent
       * an empty entry view, hence {@link ReadEntryView#find()} will return
       * an empty {@link java.util.Optional} instance, and
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
       *
       * @since 8.0
       */
      @Experimental
      interface WriteListener<K, V> {
         /**
          * Entry write event callback that receives a {@link ReadEntryView}
          * of the written entry.
          *
          * <p>For created or modified/updated events, the
          * {@link ReadEntryView} passed in will represent the newly stored
          * entry, hence implementations will not be available to differentiate
          * between created events vs modified/updated events.
          *
          * <p>For removed events, {@link ReadEntryView} passed in will represent
          * an empty entry view, hence {@link ReadEntryView#find()} will return
          * an empty {@link java.util.Optional} instance, and
          * {@link ReadEntryView#get()} will throw
          * {@link java.util.NoSuchElementException}.
          *
          * @param write written entry view
          */
         void onWrite(ReadEntryView<K, V> write);
      }
   }

}
