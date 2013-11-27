package org.infinispan.context;

import org.infinispan.container.entries.CacheEntry;

import java.util.Map;

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
    * @return a map of looked up entries.
    */
   Map<Object, CacheEntry> getLookedUpEntries();

   /**
    * Puts an entry in the registry of looked up entries in the current scope.
    * <p/>
    *
    * @param key key to store
    * @param e   entry to store
    */
   void putLookedUpEntry(Object key, CacheEntry e);

   void removeLookedUpEntry(Object key);

}
