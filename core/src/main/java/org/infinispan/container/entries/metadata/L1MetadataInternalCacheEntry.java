package org.infinispan.container.entries.metadata;

import org.infinispan.metadata.Metadata;

/**
 * A {@link org.infinispan.container.entries.InternalCacheEntry} implementation to store a L1 entry.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class L1MetadataInternalCacheEntry extends MetadataMortalCacheEntry {

   public L1MetadataInternalCacheEntry(Object key, Object value, Metadata metadata, long created) {
      super(key, value, metadata, created);
   }

   @Override
   public boolean isL1Entry() {
      return true;
   }
}
