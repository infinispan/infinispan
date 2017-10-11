package org.infinispan.conflict;

import org.infinispan.container.entries.CacheEntry;

/**
 * @author Ryan Emerson
 * @since 9.1
 */
public class ClusterMergePolicies {

   public static final EntryMergePolicy PREFERRED_ALWAYS = (preferredEntry, otherEntries) -> preferredEntry;

   public static final EntryMergePolicy PREFERRED_NON_NULL = (preferredEntry, otherEntries) -> {
      if (preferredEntry != null || otherEntries.isEmpty()) return preferredEntry;

      return (CacheEntry) otherEntries.get(0);
   };

   public static final EntryMergePolicy REMOVE_ALL = (preferredEntry, otherEntries) -> null;
}
