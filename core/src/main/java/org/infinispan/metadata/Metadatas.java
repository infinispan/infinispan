package org.infinispan.metadata;

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

}
