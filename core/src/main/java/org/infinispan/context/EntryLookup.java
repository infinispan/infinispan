package org.infinispan.context;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;

import org.infinispan.container.entries.CacheEntry;

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
    *
    * @deprecated Since 9.3, please use {@link #forEachEntry(BiConsumer)} or {@link #lookedUpEntriesSize()} instead.
    */
   @Deprecated
   Map<Object, CacheEntry> getLookedUpEntries();

   default void forEachEntry(BiConsumer<Object, CacheEntry> consumer) {
      getLookedUpEntries().forEach(consumer);
   }

   default int lookedUpEntriesSize() {
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
