package org.infinispan.functional.impl;

import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.api.functional.Listeners.ReadWriteListeners;
import org.infinispan.commons.api.functional.Listeners.WriteListeners;

/**
 * Listener notifier
 *
 * @since 8.0
 */
public interface ListenerNotifier<K, V> extends ReadWriteListeners<K, V>, WriteListeners<K, V> {

   /**
    * Notify registered {@link ReadWriteListener} instances of the created entry.
    */
   void notifyOnCreate(ReadEntryView<K, V> created);

   /**
    * Notify registered {@link ReadWriteListener} instances of the modified
    * entry passing the previous and new value.
    */
   void notifyOnModify(ReadEntryView<K, V> before, ReadEntryView<K, V> after);

   /**
    * Notify registered {@link ReadWriteListener} instances of the removed
    * entry passing in the removed entry.
    */
   void notifyOnRemove(ReadEntryView<K, V> removed);

   /**
    * Notify registered {@link WriteListener} instances of the written entry.
    */
   void notifyOnWrite(ReadEntryView<K, V> write);

}
