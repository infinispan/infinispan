package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.impl.ReturnValueStage;

/**
 * Callback interface for {@link InvocationStage#exceptionally(InvocationExceptionHandler)}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@FunctionalInterface
public interface InvocationExceptionHandler extends InvocationComposeHandler {
   Object apply(InvocationContext rCtx, VisitableCommand rCommand, Throwable t) throws Throwable;

   @Override
   default BasicInvocationStage apply(BasicInvocationStage stage, InvocationContext rCtx, VisitableCommand rCommand, Object rv,
         Throwable t) throws Throwable {
      if (t == null) return stage;

      return new ReturnValueStage(rCtx, rCommand, apply(rCtx, rCommand, t));
   }
}
