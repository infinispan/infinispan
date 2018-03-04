package org.infinispan.functional.impl;

import java.util.function.Supplier;

import org.infinispan.commons.util.Experimental;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.Listeners.ReadWriteListeners;
import org.infinispan.metadata.Metadata;

/**
 * Listener notifier.
 *
 * @since 8.0
 */
@Scope(Scopes.NAMED_CACHE)
@Experimental
public interface FunctionalNotifier<K, V> extends ReadWriteListeners<K, V> {

   /**
    * Notify registered {@link ReadWriteListener} instances of the created entry.
    */
   void notifyOnCreate(CacheEntry<K, V> entry);

   /**
    * Notify registered {@link ReadWriteListener} instances of the modified
    * entry passing the previous and new value.
    */
   void notifyOnModify(CacheEntry<K, V> entry, V previousValue, Metadata previousMetadata);

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
   void notifyOnWriteRemove(K key);

   void notifyOnWrite(CacheEntry<K, V> entry);

}
