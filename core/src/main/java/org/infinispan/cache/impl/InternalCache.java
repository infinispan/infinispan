package org.infinispan.cache.impl;

import org.infinispan.context.InvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.security.SecureMethod;

/**
 * This interface is used to hold methods that are only available to internal implementations.
 * @since 15.0
 **/
public interface InternalCache<K, V> {
   ComponentRegistry getComponentRegistry();

   /**
    * Whether to bypass the {@link org.infinispan.context.InvocationContextFactory} and utilize the
    * {@link #readContext(DecoratedCache, int)} and {@link #writeContext(DecoratedCache, int)} to
    * create the {@link InvocationContext} instance.
    *
    * @return <code>true</code> to bypass, <code>false</code>, otherwise.
    */
   @SecureMethod
   default boolean bypassInvocationContextFactory() {
      return false;
   }

   /**
    * Creates an {@link InvocationContext} using the cache instance for a read operation.
    *
    * @param cache Decorated cache to create the context.
    * @param size Number of keys.
    * @return a {@link InvocationContext} to utilize for the command execution.
    */
   static InvocationContext readContext(DecoratedCache<?, ?> cache, int size) {
      return cache.readContext(size);
   }

   /**
    * Creates an {@link InvocationContext} using the cache instance for a write operation.
    *
    * @param cache Decorated cache to create the context.
    * @param size Number of keys.
    * @return a {@link InvocationContext} to utilize for the command execution.
    */
   static InvocationContext writeContext(DecoratedCache<?, ?> cache, int size) {
      return cache.writeContext(size);
   }
}
