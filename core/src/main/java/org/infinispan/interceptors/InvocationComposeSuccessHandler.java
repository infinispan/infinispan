package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * Callback interface for {@link InvocationStage#thenCompose(InvocationContext, VisitableCommand, InvocationComposeSuccessHandler)}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@FunctionalInterface
public interface InvocationComposeSuccessHandler extends InvocationComposeHandler {
   InvocationStage apply(InvocationStage stage, InvocationContext rCtx, VisitableCommand rCommand, Object rv) throws Throwable;

   @Override
   default InvocationStage apply(InvocationStage stage, InvocationContext rCtx, VisitableCommand rCommand,
                                 Object rv, Throwable t) throws Throwable {
      if (t != null) return stage;

      return apply(stage, rCtx, rCommand, rv);
   }
}
