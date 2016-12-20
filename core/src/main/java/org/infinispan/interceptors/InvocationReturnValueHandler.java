package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.impl.InvocationStageImpl;

/**
 * Callback interface for {@link InvocationStage#thenApply(InvocationContext, VisitableCommand, InvocationReturnValueHandler)}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@FunctionalInterface
public interface InvocationReturnValueHandler extends InvocationComposeHandler {
   Object apply(InvocationContext rCtx, VisitableCommand rCommand, Object rv)
         throws Throwable;

   @Override
   default InvocationStage apply(InvocationStage stage, InvocationContext rCtx, VisitableCommand rCommand, Object rv,
                                 Throwable t) throws Throwable {
      if (t != null) return stage;

      try {
         Object result = apply(rCtx, rCommand, rv);
         return InvocationStageImpl.makeSuccessful(result);
      } catch (Throwable t1) {
         return InvocationStageImpl.makeExceptional(t1);
      }
   }
}
