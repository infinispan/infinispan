package org.infinispan.container.entries.metadata;

import java.util.Optional;

import org.infinispan.metadata.Metadata;

/**
 * Marker interface for metadata aware cache entry.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public interface MetadataAware {

   /**
    * Get metadata of this cache entry.
    *
    * @return a Metadata instance
    */
   Metadata getMetadata();

   default Optional<Metadata> metadata() {
      return Optional.ofNullable(getMetadata());
   }

   /**
    * Set the metadata in the cache entry.
    *
    * @param metadata to apply to the cache entry
    */
   void setMetadata(Metadata metadata);

}
