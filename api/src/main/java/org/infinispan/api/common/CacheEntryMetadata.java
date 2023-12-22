package org.infinispan.api.common;

import java.time.Instant;
import java.util.Optional;

/**
 * @since 14.0
 **/
public interface CacheEntryMetadata {
   /**
    * If the entry is mortal, returns an {@link Instant} representing its creation time,
    * If the entry is immortal, returns {@link Optional#empty()}
    */
   Optional<Instant> creationTime();

   Optional<Instant> lastAccessTime();

   CacheEntryExpiration expiration();

   CacheEntryVersion version();
}
