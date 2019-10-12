package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * Callback interface for {@link BaseAsyncInterceptor#invokeNextThenApply(InvocationContext, VisitableCommand, InvocationSuccessFunction)}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@FunctionalInterface
public interface InvocationSuccessFunction<C extends VisitableCommand> extends InvocationCallback<C> {
   /**
    * Process the result from a successful invocation stage and either return a simple value,
    * return a new {@link InvocationStage}, or throw an exception.
    */
   Object apply(InvocationContext rCtx, C rCommand, Object rv) throws Throwable;

   @Override
   default Object apply(InvocationContext rCtx, C rCommand, Object rv, Throwable throwable) throws Throwable {
      if (throwable != null)
         throw throwable;

      return apply(rCtx, rCommand, rv);
   }
}
