package org.infinispan.metadata;

import org.infinispan.container.entries.CacheEntry;

/**
 * Utility method for Metadata classes.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class Metadatas {

   private Metadatas() {
   }

   /**
    * Applies version in source metadata to target metadata, if no version
    * in target metadata. This method can be useful in scenarios where source
    * version information must be kept around, i.e. write skew, or when
    * reading metadata from cache store.
    *
    * @param source Metadata object which is source, whose version might be
    *               is of interest for the target metadata
    * @param target Metadata object on which version might be applied
    * @return either, the target Metadata instance as it was when it was
    * called, or a brand new target Metadata instance with version from source
    * metadata applied.
    */
   public static Metadata applyVersion(Metadata source, Metadata target) {
      if (target.version() == null && source.version() != null)
         return target.builder().version(source.version()).build();

      return target;
   }

   /**
    * Set the {@code providedMetadata} on the cache entry.
    *
    * If the entry already has a version, copy the version in the new metadata.
    */
   public static void updateMetadata(CacheEntry entry, Metadata providedMetadata) {
      if (entry != null && providedMetadata != null) {
         Metadata mergedMetadata;
         if (entry.getMetadata() == null) {
            mergedMetadata = providedMetadata;
         } else {
            mergedMetadata = applyVersion(entry.getMetadata(), providedMetadata);
         }
         entry.setMetadata(mergedMetadata);
      }
   }
}
