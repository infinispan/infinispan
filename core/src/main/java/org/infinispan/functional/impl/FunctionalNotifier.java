package org.infinispan.functional.impl;

import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.api.functional.Listeners.ReadWriteListeners;
import org.infinispan.commons.api.functional.Listeners.WriteListeners;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

import java.util.function.Supplier;

/**
 * Listener notifier.
 *
 * @since 8.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface FunctionalNotifier<K, V> extends ReadWriteListeners<K, V> {

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
    *
    * @apiNote By using a {@link Supplier} the entry view can be computed lazily
    * only if any listeners has been registered.
    */
   void notifyOnWrite(Supplier<ReadEntryView<K, V>> write);

}
