package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * Callback interface for {@link InvocationStage#thenAccept(InvocationSuccessHandler)}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@FunctionalInterface
public interface InvocationSuccessHandler extends InvocationComposeHandler {
   void accept(InvocationContext rCtx, VisitableCommand rCommand, Object rv) throws Throwable;

   @Override
   default BasicInvocationStage apply(BasicInvocationStage stage, InvocationContext rCtx, VisitableCommand rCommand, Object rv,
         Throwable t) throws Throwable {
      if (t == null) {
         accept(rCtx, rCommand, rv);
      }
      return stage;
   }
}
