package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * Callback interface for {@link BaseAsyncInterceptor#invokeNextAndExceptionally(InvocationContext, VisitableCommand, InvocationExceptionFunction)}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@FunctionalInterface
public interface InvocationExceptionFunction extends InvocationCallback {
   /**
    * Process the result from a successful invocation stage and either return a simple value,
    * return a new {@link InvocationStage}, or throw an exception.
    */
   Object apply(InvocationContext rCtx, VisitableCommand rCommand, Throwable throwable) throws Throwable;

   @Override
   default Object apply(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable throwable) throws Throwable {
      if (throwable == null)
         return rv;

      return apply(rCtx, rCommand, throwable);
   }
}
