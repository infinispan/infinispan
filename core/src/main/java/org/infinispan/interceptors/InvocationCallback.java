package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * Base interface for all callbacks used by {@link BaseAsyncInterceptor} and {@link InvocationStage} methods.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@FunctionalInterface
public interface InvocationCallback {
   /**
    * Process the result or the exception from an invocation stage and either return a simple value,
    * return a new {@link InvocationStage}, or throw an exception.
    */
   Object apply(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable throwable) throws Throwable;
}
