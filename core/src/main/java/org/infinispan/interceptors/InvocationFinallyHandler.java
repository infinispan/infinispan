package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * Callback interface for {@link InvocationStage#handle(InvocationFinallyHandler)}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@FunctionalInterface
public interface InvocationFinallyHandler extends InvocationComposeHandler {
   void accept(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable t) throws Throwable;

   @Override
   default BasicInvocationStage apply(BasicInvocationStage stage, InvocationContext rCtx,
                                  VisitableCommand rCommand, Object rv, Throwable t)
         throws Throwable {
      accept(rCtx, rCommand, rv, t);
      return stage;
   }
}
