package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * Callback interface for {@link BaseAsyncInterceptor#invokeNextThenAccept(InvocationContext, VisitableCommand, InvocationSuccessAction)}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@FunctionalInterface
public interface InvocationSuccessAction<C extends VisitableCommand> extends InvocationCallback<C> {
   /**
    * Process the result from a successful invocation stage and possibly throw an exception.
    */
   void accept(InvocationContext rCtx, C rCommand, Object rv) throws Throwable;

   @Override
   default Object apply(InvocationContext rCtx, C rCommand, Object rv, Throwable throwable) throws Throwable {
      if (throwable == null) {
         accept(rCtx, rCommand, rv);
         return rv;
      } else {
         throw throwable;
      }
   }
}
