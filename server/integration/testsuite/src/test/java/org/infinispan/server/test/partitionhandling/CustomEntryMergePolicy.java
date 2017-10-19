package org.infinispan.server.test.partitionhandling;

import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;

import java.util.List;

public class CustomEntryMergePolicy implements EntryMergePolicy {

   @Override
   public CacheEntry merge(CacheEntry preferredEntry, List otherEntries) {
      return TestInternalCacheEntryFactory.create(preferredEntry.getKey(), "customValue");
   }

}
