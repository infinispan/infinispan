package org.infinispan.api.common.process;

import org.infinispan.api.Experimental;

/**
 * Write result for process operations on the Cache
 *
 * @since 14.0
 */
@Experimental
public interface CacheEntryProcessorResult<K, T> {
   K key();

   T result();
}
