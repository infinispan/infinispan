package org.infinispan.eviction;

import org.infinispan.configuration.cache.MemoryConfiguration;

/**
 * Supported eviction type
 *
 * @author Tristan Tarrant
 * @since 8.0
 * @deprecated Since 11.0, {@link MemoryConfiguration#maxCount()} and
 * {@link MemoryConfiguration#maxSize()} should be used to differentiate between
 * the eviction thresholds.
 */
@Deprecated
public enum EvictionType {
   COUNT,
   MEMORY,
}
