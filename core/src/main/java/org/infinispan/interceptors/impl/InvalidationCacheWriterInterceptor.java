package org.infinispan.interceptors.impl;

import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.configuration.cache.CacheType;
import org.infinispan.context.InvocationContext;

/**
 * Extends the {@link CacheWriterInterceptor} to add logic for {@link CacheType#INVALIDATION}.
 * <p>
 * With invalidation, the {@link RemoveCommand} should always try to remove from persistence, even if the originator
 * does not have the data in memory. The removing non-existing value logic has an optimization that skips the
 * persistence removal; it is disabled in this interceptor.
 *
 * @since 16.0
 */
public class InvalidationCacheWriterInterceptor extends CacheWriterInterceptor {

   @Override
   boolean shouldReplicateRemove(InvocationContext ctx, RemoveCommand removeCommand) {
      // the key always need to be removed from the persistence in invalidation mode.
      return true;
   }
}
