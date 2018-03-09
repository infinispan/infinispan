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
         Metadata prevMetadata = entry.getMetadata();
         if (prevMetadata == null) {
            mergedMetadata = providedMetadata;
         } else {
            Metadata.Builder builder = providedMetadata.builder();
            if (providedMetadata.version() == null && prevMetadata.version() != null) {
               builder = providedMetadata.builder().version(prevMetadata.version());
            }
            mergedMetadata = builder.invocations(prevMetadata.lastInvocation()).build();
         }
         entry.setMetadata(mergedMetadata);
      }
   }

   public static Metadata.Builder merged(Metadata prevMetadata, Metadata providedMetadata) {
      Metadata.Builder builder;
      if (prevMetadata == null) {
         if (providedMetadata == null) {
            builder = new EmbeddedMetadata.Builder();
         } else {
            builder = providedMetadata.builder();
         }
      } else if (providedMetadata == null) {
         builder = prevMetadata.builder();
      } else {
         builder = providedMetadata.builder().invocations(prevMetadata.lastInvocation());
         if (providedMetadata.version() == null && prevMetadata.version() != null) {
            builder = builder.version(prevMetadata.version());
         }
      }
      return builder;
   }
}
