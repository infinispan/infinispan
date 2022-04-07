package org.infinispan.api.common.process;

import org.infinispan.api.common.CacheOptions;

/**
 * @since 14.0
 **/
public interface CacheEntryProcessorContext {
   Object[] arguments();

   CacheOptions options();
}
