package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * Callback interface for {@link BaseAsyncInterceptor#invokeNextAndFinally(InvocationContext, VisitableCommand, InvocationFinallyAction)}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@FunctionalInterface
public interface InvocationFinallyAction extends InvocationCallback {
   /**
    * Process the result or the exception from an invocation stage and possibly throw an exception.
    */
   void accept(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable throwable) throws Throwable;

   @Override
   default Object apply(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable throwable) throws Throwable {
      accept(rCtx, rCommand, rv, throwable);
      if (throwable == null) {
         return rv;
      } else {
         throw throwable;
      }
   }
}
