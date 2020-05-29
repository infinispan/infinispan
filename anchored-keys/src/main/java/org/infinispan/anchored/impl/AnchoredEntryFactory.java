package org.infinispan.anchored.impl;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.impl.EntryFactoryImpl;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * Store the key location in context entries.
 *
 * @author Dan Berindei
 * @since 11
 */
public class AnchoredEntryFactory extends EntryFactoryImpl {
   @Override
   protected MVCCEntry<?, ?> createWrappedEntry(Object key, CacheEntry<?, ?> cacheEntry) {
      Object value = null;
      Metadata metadata = null;
      PrivateMetadata internalMetadata = null;
      if (cacheEntry != null) {
         value = cacheEntry.getValue();
         metadata = cacheEntry.getMetadata();
         internalMetadata = cacheEntry.getInternalMetadata();
      }

      MVCCEntry<?, ?> mvccEntry;
      mvccEntry = new AnchoredReadCommittedEntry(key, value, metadata);
      mvccEntry.setInternalMetadata(internalMetadata);
      if (cacheEntry != null) {
         mvccEntry.setCreated(cacheEntry.getCreated());
         mvccEntry.setLastUsed(cacheEntry.getLastUsed());
      }
      return mvccEntry;
   }
}
