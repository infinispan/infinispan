package org.infinispan.context;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;

import org.infinispan.container.entries.CacheEntry;
import org.reactivestreams.Publisher;

/**
 * Interface that can look up MVCC wrapped entries.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public interface EntryLookup {
   /**
    * Retrieves an entry from the collection of looked up entries in the current scope.
    * <p/>
    *
    * @param key key to look up
    * @return an entry, or null if it cannot be found.
    */
   CacheEntry lookupEntry(Object key);

   /**
    * Retrieves a map of entries looked up within the current scope.
    * <p/>
    * Note: The key inside the {@linkplain CacheEntry} may be {@code null} if the key does not exist in the cache.
    *
    * @return a map of looked up entries.
    */
   Map<Object, CacheEntry> getLookedUpEntries();

   /**
    * Returns a Publisher that when subscribed to provide all values that have a value in the given context.
    *
    * @param <K> key type provided from user
    * @param <V> value type provided from user
    * @return
    */
   <K, V> Publisher<CacheEntry<K, V>> publisher();

   /**
    * Execute an action for each value in the context.
    * <p>
    * Entries that do not have a value (because the key was removed, or it doesn't exist in the cache).
    *
    * @since 9.3
    */
   default void forEachValue(BiConsumer<Object, CacheEntry> action) {
      forEachEntry((key, entry) -> {
         if (!entry.isRemoved() && !entry.isNull()) {
            action.accept(key, entry);
         }
      });
   }

   /**
    * Execute an action for each entry in the context.
    *
    * Includes invalid entries, which have a {@code null} value and may also report a {@code null} key.
    *
    * @since 9.3
    */
   default void forEachEntry(BiConsumer<Object, CacheEntry> action) {
      getLookedUpEntries().forEach(action);
   }

   /**
    * @return The number of entries wrapped in the context, including invalid entries.
    */
   default int lookedUpEntriesCount() {
      return getLookedUpEntries().size();
   }

   /**
    * Puts an entry in the registry of looked up entries in the current scope.
    * <p/>
    *
    * @param key key to store
    * @param e   entry to store
    */
   void putLookedUpEntry(Object key, CacheEntry e);

   void removeLookedUpEntry(Object key);

   default void removeLookedUpEntries(Collection<?> keys) {
      for (Object key : keys) {
         removeLookedUpEntry(key);
      }
   }
}
