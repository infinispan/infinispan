package org.infinispan.conflict;

import java.util.List;

import org.infinispan.container.entries.CacheEntry;

public enum MergePolicy implements EntryMergePolicy {
   CUSTOM(),
   NONE(),
   PREFERRED_ALWAYS((preferredEntry, otherEntries) -> preferredEntry),

   PREFERRED_NON_NULL((preferredEntry, otherEntries) -> {
            if (preferredEntry != null || otherEntries.isEmpty()) return preferredEntry;

            return (CacheEntry) otherEntries.get(0);
         }),

   REMOVE_ALL((preferredEntry, otherEntries) -> null);

   private final EntryMergePolicy impl;
   MergePolicy() {
      this(new UnsupportedMergePolicy());
   }

   MergePolicy(EntryMergePolicy policy) {
      this.impl = policy;
   }

   @Override
   public CacheEntry merge(CacheEntry preferredEntry, List otherEntries) {
      return impl.merge(preferredEntry, otherEntries);
   }

   public static MergePolicy fromString(String str) {
      for (MergePolicy mp : MergePolicy.values())
         if (mp.name().equalsIgnoreCase(str))
            return mp;
      return CUSTOM;
   }

   public static MergePolicy fromConfiguration(EntryMergePolicy policy) {
      if (policy == null) return NONE;

      for (MergePolicy mp : MergePolicy.values())
         if (mp == policy)
            return mp;
      return CUSTOM;
   }

   public static class UnsupportedMergePolicy implements EntryMergePolicy {
      @Override
      public CacheEntry merge(CacheEntry preferredEntry, List otherEntries) {
         throw new UnsupportedOperationException();
      }
   }
}
