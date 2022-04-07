package org.infinispan.api.common;

import java.time.Instant;
import java.util.Optional;

/**
 * @since 14.0
 **/
public interface CacheEntryMetadata {

   Optional<Instant> creationTime();

   Optional<Instant> lastAccessTime();

   CacheEntryExpiration expiration();

   CacheEntryVersion version();
}
