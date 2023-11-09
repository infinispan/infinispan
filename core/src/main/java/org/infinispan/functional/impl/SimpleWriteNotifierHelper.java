package org.infinispan.functional.impl;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.impl.ImmutableContext;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.concurrent.CompletionStages;

/**
 * Notification mechanism for the simple functional map writes.
 * This mechanism notifies both cache and functional map listeners. Sending notification for create, remove, and updates.
 * The implementation joins on the future returned by the cache notification.
 *
 * @see org.infinispan.notifications.cachelistener.NotifyHelper
 */
final class SimpleWriteNotifierHelper {

   private SimpleWriteNotifierHelper() { }

   static <K, V> void handleNotification(CacheNotifier<K, V> cacheNotifier, FunctionalNotifier<K, V> functionalNotifier,
                                         K key, EntryHolder<K, V> holder, boolean pre) {
      if (holder == null) return;

      CacheEntry<K, V> oldEntry = holder.before;
      MVCCEntry<K, V> e = holder.after;

      if (e.isRemoved()) {
         CompletionStages.join(cacheNotifier.notifyCacheEntryRemoved(key, oldEntry.getValue(), oldEntry.getMetadata(), pre, ImmutableContext.INSTANCE, null));

         if (!pre) {
            if (!holder.isWriteOnly)
               functionalNotifier.notifyOnRemove(EntryViews.readOnly(key, oldEntry.getValue(), oldEntry.getMetadata()));

            functionalNotifier.notifyOnWriteRemove(key);
         }
      } else {
         // Creating new entry.
         if (oldEntry == null) {
            CompletionStages.join(cacheNotifier.notifyCacheEntryCreated(key, e.getValue(), e.getOldMetadata(), pre, ImmutableContext.INSTANCE, null));

            if (!pre) {
               if (!holder.isWriteOnly)
                  functionalNotifier.notifyOnCreate(e);

               functionalNotifier.notifyOnWrite(e);
            }

         // Entry was updated.
         } else {
            CompletionStages.join(cacheNotifier.notifyCacheEntryModified(key, e.getValue(), e.getMetadata(), oldEntry.getValue(), oldEntry.getMetadata(), pre, ImmutableContext.INSTANCE, null));

            if (!pre) {
               if (!holder.isWriteOnly)
                  functionalNotifier.notifyOnModify(e, oldEntry.getValue(), oldEntry.getMetadata());

               functionalNotifier.notifyOnWrite(e);
            }
         }
      }
   }

   static <K, V> EntryHolder<K, V> create(CacheEntry<K, V> before, MVCCEntry<K, V> after) {
      return new EntryHolder<>(before, after, false);
   }

   static <K, V> EntryHolder<K, V> createWriteOnly(CacheEntry<K, V> before, MVCCEntry<K, V> after) {
      return new EntryHolder<>(before, after, true);
   }

   static class EntryHolder<K, V> {
      private final CacheEntry<K, V> before;
      private final MVCCEntry<K, V> after;
      private final boolean isWriteOnly;

      private EntryHolder(CacheEntry<K, V> before, MVCCEntry<K, V> after, boolean isWriteOnly) {
         this.before = before;
         this.after = after;
         this.isWriteOnly = isWriteOnly;
      }
   }
}
