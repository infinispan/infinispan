package org.infinispan.cache.impl;

import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;

/**
 * An interface to build {@link InvocationContext}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@FunctionalInterface
public interface ContextBuilder {

   /**
    * Creates a new {@link InvocationContext}.
    * <p>
    * The {@code keyCount} specifies the number of keys affected that this context will handle. Use {@link
    * InvocationContextFactory#UNBOUNDED} to specify an unbound number of keys.
    * <p>
    * Some implementation may ignore {@code keyCount}.
    *
    * @param keyCount The number of keys affected.
    * @return An {@link InvocationContext} to use.
    */
   InvocationContext create(int keyCount);
}
