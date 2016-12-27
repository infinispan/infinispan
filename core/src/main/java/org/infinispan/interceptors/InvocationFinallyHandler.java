package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * Callback interface for {@link InvocationStage#handle(InvocationContext, VisitableCommand, InvocationFinallyHandler)}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@FunctionalInterface
public interface InvocationFinallyHandler extends InvocationComposeHandler {
   void accept(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable t) throws Throwable;

   @Override
   default InvocationStage apply(InvocationStage stage, InvocationContext rCtx,
                                 VisitableCommand rCommand, Object rv, Throwable t)
         throws Throwable {
      accept(rCtx, rCommand, rv, t);
      return stage;
   }
}
